"""Transaction decoder — receipt-based trade classification.

Architecture:
  Instead of watching specific contract addresses, the listener monitors
  BEP-20 Transfer events to/from the target wallet. For each relevant
  transaction, the full receipt is fetched and ALL logs are analysed to
  classify the trade type:

    1. Bonding-curve trades  (fourmeme / flap platform events)
    2. DEX V2 swaps          (PancakeSwap, BiSwap, etc.)
    3. DEX V3 swaps          (PancakeSwap V3, Uniswap V3 forks)
    4. Simple transfers      (no swap event — just token movement)

  Trade direction is determined by token flow relative to the target:
    - BNB/WBNB out + token in  →  buy
    - Token out + BNB/WBNB in  →  sell
    - Token A out + Token B in →  swap
"""

from __future__ import annotations

from dataclasses import dataclass, field

from core.constants import PLATFORM_CONTRACTS, WBNB_ADDRESS

# ---------------------------------------------------------------------------
# BEP-20 Transfer event  (same as ERC-20)
# Transfer(address indexed from, address indexed to, uint256 value)
# ---------------------------------------------------------------------------
TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

# ---------------------------------------------------------------------------
# Platform contract addresses (lower-case for matching)
# ---------------------------------------------------------------------------
_FOURMEME = PLATFORM_CONTRACTS.get("fourmeme", "").lower()
_FLAP     = PLATFORM_CONTRACTS.get("flap", "").lower()

# ---------------------------------------------------------------------------
# Flap event topic0 hashes
# ---------------------------------------------------------------------------
FLAP_TOPIC_BOUGHT  = "0xa800a2038683844fac66747f771bfdfae862eb28b16bcfa387afa9fbacce8ff7"
FLAP_TOPIC_SOLD    = "0x03a4693e592f5e75dc7c136acb39b146d2b4966c0e509c34f362dee02b3b861a"
FLAP_TOPIC_CREATED = "0x504e7f360b2e5fe33cbaaae4c593bc55305328341bf79009e43e0e3b7f699603"
FLAP_TOPIC_STAGED  = "0x2bf0a17cc6127084d945eb95a40df6c839234845722b025fdeef767b9464c02d"

# ---------------------------------------------------------------------------
# Four.meme event topic0 hashes
# ---------------------------------------------------------------------------
FOURMEME_TOPIC_BUY_V1    = "0x00fe0e12b43090c1fc19a34aefa5cc138a4eeafc60ab800f855c730b3fb9480e"
FOURMEME_TOPIC_BUY_V2    = "0xc29b8032387f267ddc010037627574acbf3b1a65a6022ca8ba6c25f0ba85ee75"
FOURMEME_TOPIC_BUY_V3    = "0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942"
FOURMEME_TOPIC_SELL_V1   = "0x80d4e495cda89b31af98c8e977ff11f417bafcee26902a17a15be51830c47533"
FOURMEME_TOPIC_SELL_V2   = "0xf4e5c9bf832eeae776f28be73b1a6c9136189b4bd81f970646f71884035bfdd4"
FOURMEME_TOPIC_SELL_V3   = "0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19"
FOURMEME_TOPIC_CREATE    = "0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20"

# ---------------------------------------------------------------------------
# DEX Swap event topics
# ---------------------------------------------------------------------------
SWAP_V2_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"
SWAP_V3_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"
SWAP_V3_TOPIC_PANCAKE = "0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83"

# ---------------------------------------------------------------------------
# Convenient sets for quick lookup
# ---------------------------------------------------------------------------
_FLAP_TOPICS = {FLAP_TOPIC_BOUGHT, FLAP_TOPIC_SOLD, FLAP_TOPIC_CREATED, FLAP_TOPIC_STAGED}
_FOURMEME_BUY_TOPICS  = {FOURMEME_TOPIC_BUY_V1, FOURMEME_TOPIC_BUY_V2, FOURMEME_TOPIC_BUY_V3}
_FOURMEME_SELL_TOPICS = {FOURMEME_TOPIC_SELL_V1, FOURMEME_TOPIC_SELL_V2, FOURMEME_TOPIC_SELL_V3}
_FOURMEME_TOPICS = _FOURMEME_BUY_TOPICS | _FOURMEME_SELL_TOPICS | {FOURMEME_TOPIC_CREATE}
_WBNB = WBNB_ADDRESS.lower()


def _token_platform(token_addr: str) -> str:
    addr = token_addr.lower()
    if addr.endswith("4444"):
        return "fourmeme"
    if addr.endswith("7777"):
        return "flap"
    return "dex"


@dataclass
class TradeEvent:
    platform: str       
    action: str         
    token: str          
    trader: str         
    amount_bnb: float   
    amount_token: int   
    tx_hash: str
    block_number: int
    extra: dict = field(default_factory=dict)


class ReceiptDecoder:
    def decode_receipt(
        self,
        tx_hash: str,
        tx_from: str,
        logs: list[dict],
        target: str,
        block_number: int,
        tx_value: int = 0,
        tx_to: str = "",
    ) -> list[TradeEvent]:
        
        events = self._check_fourmeme(tx_hash, tx_from, logs, target, block_number, tx_value)
        if events:
            return events

        events = self._check_flap(tx_hash, tx_from, logs, target, block_number, tx_value)
        if events:
            return events

        events = self._check_dex_swaps(tx_hash, tx_from, logs, target, block_number, tx_value, tx_to)
        if events:
            return events

        return self._check_transfers(tx_hash, tx_from, logs, target, block_number)

    def _check_fourmeme(
        self, tx_hash: str, tx_from: str, logs: list[dict], target: str,
        block_number: int, tx_value: int = 0,
    ) -> list[TradeEvent]:
        wbnb_to_target = _sum_wbnb_transfers_to(logs, target)
        if wbnb_to_target == 0.0:
            wbnb_to_target = _sum_wbnb_from_pairs(_extract_transfers(logs)) / 1e18

        target_set = {target}
        if tx_from == target:
            target_set = None  

        events: list[TradeEvent] = []
        for log in logs:
            addr = _log_addr(log)
            if addr != _FOURMEME:
                continue
            topic0 = _topic0(log)
            if topic0 not in _FOURMEME_TOPICS:
                continue
            data = _strip_hex(log.get("data") or "0x")

            if topic0 in _FOURMEME_BUY_TOPICS:
                token   = _addr_at(data, 0)
                account = _addr_at(data, 1)
                if target_set is not None and account not in target_set:
                    continue
                if topic0 == FOURMEME_TOPIC_BUY_V3:
                    amount_token = _uint_at(data, 3)
                    event_quote  = _uint_at(data, 4) / 1e18
                    fee_wei      = _uint_at(data, 5)
                else:
                    amount_token = _uint_at(data, 2)
                    event_quote  = _uint_at(data, 3) / 1e18
                    fee_wei      = _uint_at(data, 4)
                
                amount_bnb = tx_value / 1e18 if tx_value > 0 else event_quote
                events.append(TradeEvent(
                    platform="fourmeme", action="buy",
                    token=token, trader=account,
                    amount_bnb=amount_bnb, amount_token=amount_token,
                    tx_hash=tx_hash, block_number=block_number,
                    extra={"fee_wei": fee_wei, "quote": "bnb"},
                ))

            elif topic0 in _FOURMEME_SELL_TOPICS:
                token   = _addr_at(data, 0)
                account = _addr_at(data, 1)
                if target_set is not None and account not in target_set:
                    continue
                if topic0 == FOURMEME_TOPIC_SELL_V3:
                    amount_token = _uint_at(data, 3)
                    event_quote  = _uint_at(data, 4) / 1e18
                    fee_wei      = _uint_at(data, 5)
                else:
                    amount_token = _uint_at(data, 2)
                    event_quote  = _uint_at(data, 3) / 1e18
                    fee_wei      = _uint_at(data, 4)
                
                amount_bnb = wbnb_to_target if wbnb_to_target > 0 else event_quote
                events.append(TradeEvent(
                    platform="fourmeme", action="sell",
                    token=token, trader=account,
                    amount_bnb=amount_bnb, amount_token=amount_token,
                    tx_hash=tx_hash, block_number=block_number,
                    extra={"fee_wei": fee_wei, "quote": "bnb"},
                ))

            elif topic0 == FOURMEME_TOPIC_CREATE:
                creator = _addr_at(data, 0)
                if target_set is not None and creator not in target_set:
                    continue
                token = _addr_at(data, 1)
                events.append(TradeEvent(
                    platform="fourmeme", action="create",
                    token=token, trader=creator,
                    amount_bnb=0.0, amount_token=0,
                    tx_hash=tx_hash, block_number=block_number,
                ))

        return events

    def _check_flap(
        self, tx_hash: str, tx_from: str, logs: list[dict], target: str,
        block_number: int, tx_value: int = 0,
    ) -> list[TradeEvent]:
        wbnb_to_target = _sum_wbnb_transfers_to(logs, target)

        target_set = {target}
        if tx_from == target:
            target_set = None  

        events: list[TradeEvent] = []
        for log in logs:
            addr = _log_addr(log)
            if addr != _FLAP:
                continue
            topic0 = _topic0(log)
            if topic0 not in _FLAP_TOPICS:
                continue
            data = _strip_hex(log.get("data") or "0x")

            if topic0 == FLAP_TOPIC_BOUGHT:
                token  = _addr_at(data, 1)
                buyer  = _addr_at(data, 2)
                if target_set is not None and buyer not in target_set:
                    continue
                amount_token = _uint_at(data, 3)
                event_quote  = _uint_at(data, 4) / 1e18
                amount_bnb = tx_value / 1e18 if tx_value > 0 else event_quote
                events.append(TradeEvent(
                    platform="flap", action="buy",
                    token=token, trader=buyer,
                    amount_bnb=amount_bnb, amount_token=amount_token,
                    tx_hash=tx_hash, block_number=block_number,
                    extra={"fee_wei": _uint_at(data, 5), "post_price": _uint_at(data, 6), "quote": "bnb"},
                ))

            elif topic0 == FLAP_TOPIC_SOLD:
                token  = _addr_at(data, 1)
                seller = _addr_at(data, 2)
                if target_set is not None and seller not in target_set:
                    continue
                amount_token = _uint_at(data, 3)
                event_quote  = _uint_at(data, 4) / 1e18
                amount_bnb = wbnb_to_target if wbnb_to_target > 0 else event_quote
                events.append(TradeEvent(
                    platform="flap", action="sell",
                    token=token, trader=seller,
                    amount_bnb=amount_bnb, amount_token=amount_token,
                    tx_hash=tx_hash, block_number=block_number,
                    extra={"fee_wei": _uint_at(data, 5), "post_price": _uint_at(data, 6), "quote": "bnb"},
                ))

            elif topic0 in (FLAP_TOPIC_CREATED, FLAP_TOPIC_STAGED):
                creator = _addr_at(data, 1)
                if target_set is not None and creator not in target_set:
                    continue
                token = _addr_at(data, 3) if topic0 == FLAP_TOPIC_CREATED else _addr_at(data, 2)
                events.append(TradeEvent(
                    platform="flap", action="create",
                    token=token, trader=creator,
                    amount_bnb=0.0, amount_token=0,
                    tx_hash=tx_hash, block_number=block_number,
                ))

        return events

    def _check_dex_swaps(
        self, tx_hash: str, tx_from: str, logs: list[dict],
        target: str, block_number: int, tx_value: int = 0,
        tx_to: str = "",
    ) -> list[TradeEvent]:

        transfers_in:  list[dict] = []   
        transfers_out: list[dict] = []   
        all_transfers: list[dict] = []   
        swap_events:   list[dict] = []   

        for log in logs:
            topic0 = _topic0(log)

            if topic0 == TRANSFER_TOPIC:
                topics = log.get("topics") or []
                if len(topics) < 3:
                    continue
                from_addr  = _topic_addr(topics[1])
                to_addr    = _topic_addr(topics[2])
                token_addr = _log_addr(log)
                value      = _uint_at(_strip_hex(log.get("data") or "0x"), 0)
                transfer   = {"token": token_addr, "value": value, "from": from_addr, "to": to_addr}
                all_transfers.append(transfer)
                if to_addr == target:
                    transfers_in.append(transfer)
                if from_addr == target:
                    transfers_out.append(transfer)

            elif topic0 == SWAP_V2_TOPIC:
                swap_events.append({"type": "v2", "log": log, "pair": _log_addr(log)})
            elif topic0 in (SWAP_V3_TOPIC, SWAP_V3_TOPIC_PANCAKE):
                swap_events.append({"type": "v3", "log": log, "pair": _log_addr(log)})

        if not swap_events:
            return []

        # Target initiated the Tx: We can rely directly on Swap logs
        if tx_from == target:
            return self._decode_aggregator_swaps(tx_hash, tx_from, swap_events, all_transfers, block_number)

        tokens_in  = _aggregate_by_token([t for t in transfers_in  if t["token"] != _WBNB])
        tokens_out = _aggregate_by_token([t for t in transfers_out if t["token"] != _WBNB])
        wbnb_in    = [t for t in transfers_in  if t["token"] == _WBNB]
        wbnb_out   = [t for t in transfers_out if t["token"] == _WBNB]

        events: list[TradeEvent] = []

        if tokens_in or tokens_out:
            if tokens_in and (wbnb_out or tx_value > 0 or not tokens_out):
                for t in tokens_in:
                    if wbnb_out:
                        bnb_amount = sum(w["value"] for w in wbnb_out) / 1e18
                    elif tx_value > 0:
                        bnb_amount = tx_value / 1e18
                    else:
                        bnb_amount = 0.0
                    events.append(TradeEvent(
                        platform=_token_platform(t["token"]), action="buy",
                        token=t["token"], trader=target,
                        amount_bnb=bnb_amount, amount_token=t["value"],
                        tx_hash=tx_hash, block_number=block_number,
                        extra={"tx_from": tx_from, "quote": "bnb"},
                    ))
            elif tokens_out and (wbnb_in or not tokens_in):
                swap_pairs = {se["pair"] for se in swap_events}
                for t in tokens_out:
                    if wbnb_in:
                        bnb_amount = sum(w["value"] for w in wbnb_in) / 1e18
                    else:
                        recipients = {target}
                        if tx_to:
                            recipients.add(tx_to)
                        bnb_amount = _sum_wbnb_from_pairs_to(all_transfers, swap_pairs, recipients) / 1e18
                    events.append(TradeEvent(
                        platform=_token_platform(t["token"]), action="sell",
                        token=t["token"], trader=target,
                        amount_bnb=bnb_amount, amount_token=t["value"],
                        tx_hash=tx_hash, block_number=block_number,
                        extra={"tx_from": tx_from, "quote": "bnb"},
                    ))
            elif tokens_out and tokens_in:
                for t_in in tokens_in:
                    events.append(TradeEvent(
                        platform=_token_platform(t_in["token"]), action="swap",
                        token=t_in["token"], trader=target,
                        amount_bnb=0.0, amount_token=t_in["value"],
                        tx_hash=tx_hash, block_number=block_number,
                        extra={
                            "tx_from": tx_from,
                            "sold_tokens": [{"token": t["token"], "amount": str(t["value"])} for t in tokens_out],
                        },
                    ))
            return events
            
        return []

    def _decode_aggregator_swaps(
        self, tx_hash: str, tx_from: str, swap_events: list[dict], 
        all_transfers: list[dict], block_number: int
    ) -> list[TradeEvent]:
        
        events_dict = {}
        for swap in swap_events:
            log = swap["log"]
            ev = self._decode_swap_v2_direction(log, all_transfers) if swap["type"] == "v2" else self._decode_swap_v3_direction(log, all_transfers)
            
            if ev:
                key = (ev["action"], ev["token"])
                if key not in events_dict:
                    events_dict[key] = ev
                else:
                    events_dict[key]["amount_bnb"] += ev["amount_bnb"]
                    events_dict[key]["amount_token"] += ev["amount_token"]

        events: list[TradeEvent] = []
        for key, ev in events_dict.items():
            events.append(TradeEvent(
                platform=_token_platform(ev["token"]), action=ev["action"],
                token=ev["token"], trader=tx_from,
                amount_bnb=ev["amount_bnb"], amount_token=ev["amount_token"],
                tx_hash=tx_hash, block_number=block_number,
                extra={"tx_from": tx_from, "quote": "bnb", "via": "swap_logs"},
            ))

        return events

    def _decode_swap_v2_direction(
        self, log: dict, all_transfers: list[dict],
    ) -> dict | None:
        data = _strip_hex(log.get("data") or "0x")
        amount0In  = _uint_at(data, 0)
        amount1In  = _uint_at(data, 1)
        amount0Out = _uint_at(data, 2)
        amount1Out = _uint_at(data, 3)

        pair_addr = _log_addr(log)
        pair_tokens = _find_pair_tokens(pair_addr, all_transfers)
        if not pair_tokens or len(pair_tokens) < 2:
            return None

        token0, token1 = sorted(pair_tokens.keys(), key=lambda a: int(a, 16))
        wbnb_is_0 = (token0 == _WBNB)
        wbnb_is_1 = (token1 == _WBNB)

        if not (wbnb_is_0 or wbnb_is_1):
            if amount0In > 0:
                return {"action": "swap", "token": token1, "amount_bnb": 0.0, "amount_token": amount1Out}
            else:
                return {"action": "swap", "token": token0, "amount_bnb": 0.0, "amount_token": amount0Out}

        if wbnb_is_0:
            if amount0In > 0:
                return {"action": "buy", "token": token1, "amount_bnb": amount0In / 1e18, "amount_token": amount1Out}
            else:
                return {"action": "sell", "token": token1, "amount_bnb": amount0Out / 1e18, "amount_token": amount1In}
        else:
            if amount1In > 0:
                return {"action": "buy", "token": token0, "amount_bnb": amount1In / 1e18, "amount_token": amount0Out}
            else:
                return {"action": "sell", "token": token0, "amount_bnb": amount1Out / 1e18, "amount_token": amount0In}

    def _decode_swap_v3_direction(
        self, log: dict, all_transfers: list[dict],
    ) -> dict | None:
        data = _strip_hex(log.get("data") or "0x")
        amount0 = _int256_at(data, 0)
        amount1 = _int256_at(data, 1)

        pair_addr = _log_addr(log)
        pair_tokens = _find_pair_tokens(pair_addr, all_transfers)
        if not pair_tokens or len(pair_tokens) < 2:
            return None

        token0, token1 = sorted(pair_tokens.keys(), key=lambda a: int(a, 16))
        wbnb_is_0 = (token0 == _WBNB)
        wbnb_is_1 = (token1 == _WBNB)

        if not (wbnb_is_0 or wbnb_is_1):
            if amount0 > 0:
                return {"action": "swap", "token": token1, "amount_bnb": 0.0, "amount_token": abs(amount1)}
            else:
                return {"action": "swap", "token": token0, "amount_bnb": 0.0, "amount_token": abs(amount0)}

        if wbnb_is_0:
            if amount0 > 0:  
                return {"action": "buy", "token": token1, "amount_bnb": amount0 / 1e18, "amount_token": abs(amount1)}
            else:  
                return {"action": "sell", "token": token1, "amount_bnb": abs(amount0) / 1e18, "amount_token": amount1}
        else:
            if amount1 > 0:  
                return {"action": "buy", "token": token0, "amount_bnb": amount1 / 1e18, "amount_token": abs(amount0)}
            else:  
                return {"action": "sell", "token": token0, "amount_bnb": abs(amount1) / 1e18, "amount_token": amount0}

    def _check_transfers(
        self, tx_hash: str, tx_from: str, logs: list[dict],
        target: str, block_number: int,
    ) -> list[TradeEvent]:
        if tx_from != target:
            return []

        events: list[TradeEvent] = []
        for log in logs:
            topic0 = _topic0(log)
            if topic0 != TRANSFER_TOPIC:
                continue
            topics = log.get("topics") or []
            if len(topics) < 3:
                continue
            from_addr  = _topic_addr(topics[1])
            to_addr    = _topic_addr(topics[2])
            token_addr = _log_addr(log)
            value      = _uint_at(_strip_hex(log.get("data") or "0x"), 0)

            if token_addr == _WBNB:
                continue  

            if from_addr == target:
                events.append(TradeEvent(
                    platform="transfer", action="transfer_out",
                    token=token_addr, trader=to_addr,
                    amount_bnb=0.0, amount_token=value,
                    tx_hash=tx_hash, block_number=block_number,
                ))
            elif to_addr == target:
                events.append(TradeEvent(
                    platform="transfer", action="transfer_in",
                    token=token_addr, trader=from_addr,
                    amount_bnb=0.0, amount_token=value,
                    tx_hash=tx_hash, block_number=block_number,
                ))

        return events

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _aggregate_by_token(transfers: list[dict]) -> list[dict]:
    agg: dict[str, dict] = {}
    for t in transfers:
        if t["token"] in agg:
            agg[t["token"]]["value"] += t["value"]
        else:
            agg[t["token"]] = dict(t)
    return list(agg.values())

def _extract_transfers(logs: list[dict]) -> list[dict]:
    result = []
    for log in logs:
        if _topic0(log) != TRANSFER_TOPIC:
            continue
        topics = log.get("topics") or []
        if len(topics) < 3:
            continue
        result.append({
            "token": _log_addr(log),
            "from": _topic_addr(topics[1]),
            "to": _topic_addr(topics[2]),
            "value": _uint_at(_strip_hex(log.get("data") or "0x"), 0),
        })
    return result

def _sum_wbnb_from_pairs_to(all_transfers: list[dict], swap_pair_addrs: set[str], recipients: set[str]) -> int:
    total = 0
    for t in all_transfers:
        if t["token"] == _WBNB and t["from"] in swap_pair_addrs and t["to"] in recipients:
            total += t["value"]
    return total

def _sum_wbnb_from_pairs(all_transfers: list[dict], swap_pair_addrs: set[str] | None = None) -> int:
    if swap_pair_addrs:
        total = 0
        for t in all_transfers:
            if t["token"] == _WBNB and t["from"] in swap_pair_addrs:
                total += t["value"]
        return total
    senders = {t["from"] for t in all_transfers}
    receivers = {t["to"] for t in all_transfers}
    pairs = senders & receivers
    total = 0
    for t in all_transfers:
        if t["token"] == _WBNB and t["from"] in pairs:
            total += t["value"]
    return total

def _sum_wbnb_transfers_to(logs: list[dict], target: str) -> float:
    total = 0
    for log in logs:
        if _topic0(log) != TRANSFER_TOPIC:
            continue
        if _log_addr(log) != _WBNB:
            continue
        topics = log.get("topics") or []
        if len(topics) < 3:
            continue
        to_addr = _topic_addr(topics[2])
        if to_addr == target:
            total += _uint_at(_strip_hex(log.get("data") or "0x"), 0)
    return total / 1e18 if total > 0 else 0.0

def _log_addr(log: dict) -> str:
    return (log.get("address") or "").lower()

def _topic0(log: dict) -> str:
    topics = log.get("topics") or []
    return (topics[0] or "").lower() if topics else ""

def _topic_addr(topic_hex: str) -> str:
    t = topic_hex.lower()
    if t.startswith("0x"):
        t = t[2:]
    if len(t) < 64:
        return ""
    return "0x" + t[24:]

def _find_pair_tokens(pair_addr: str, all_transfers: list[dict]) -> dict[str, int]:
    tokens: dict[str, int] = {}
    for t in all_transfers:
        if t["from"] == pair_addr or t["to"] == pair_addr:
            if t["token"] not in tokens:
                tokens[t["token"]] = t["value"]
            else:
                tokens[t["token"]] += t["value"]
    return tokens

def _strip_hex(s: str) -> str:
    return s[2:] if s.startswith("0x") else s

def _addr_at(data: str, word_index: int) -> str:
    start = word_index * 64
    if len(data) < start + 64:
        return ""
    return "0x" + data[start + 24: start + 64].lower()

def _uint_at(data: str, word_index: int) -> int:
    start = word_index * 64
    if len(data) < start + 64:
        return 0
    try:
        return int(data[start: start + 64], 16)
    except ValueError:
        return 0

def _int256_at(data: str, word_index: int) -> int:
    val = _uint_at(data, word_index)
    if val >= (1 << 255):
        val -= (1 << 256)
    return val