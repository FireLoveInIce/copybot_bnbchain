"""Copy-trade engine.

Each copy task runs as an independent asyncio coroutine that:
  1. Registers a callback on the listener engine
  2. Receives trade events and executes copy buys/sells
  3. Optionally monitors positions for TP/SL

Buy modes
---------
  fixed : buy exactly ``buy_config.amount`` BNB worth of the token
  smart : buy based on conditions (up to 3 tiers based on target BNB spend)

Sell modes
----------
  copy_sell : sell all tokens when target sells (or swaps)
  tp_sl     : take-profit / stop-loss based on BNB value change
"""

from __future__ import annotations

import asyncio
import json
import logging

import aiosqlite
from eth_account import Account

from core.constants import TASK_STATUS_RUNNING, TASK_STATUS_PAUSED
from copytrade.router import TradeRouter
from database.db import DB_PATH
from listener.engine import ListenerEngine
from logs.service import LogService
from rpc.manager import RpcManager

logger = logging.getLogger(__name__)

# TP/SL price check interval
TPSL_CHECK_INTERVAL = 15  # seconds

# Retry settings
BUY_SLIPPAGE_TIERS = [10, 15, 20, 15, 20]  # escalating slippage per attempt
BUY_MAX_RETRIES = len(BUY_SLIPPAGE_TIERS)
SELL_SLIPPAGE_TIERS = [10, 15, 15, 20, 20]  # for sell retries
SELL_MAX_RETRIES = 50  # effectively unlimited — sell until success
RETRY_BASE_DELAY = 2  # seconds


class CopyTradeEngine:
    def __init__(
        self,
        log_service: LogService,
        rpc_manager: RpcManager,
        listener_engine: ListenerEngine,
    ):
        self.log = log_service
        self.rpc = rpc_manager
        self.listener = listener_engine
        self.router = TradeRouter(rpc_manager)

    # ------------------------------------------------------------------
    # Per-task entry point (started via RuntimeManager)
    # ------------------------------------------------------------------

    async def run_copy_task(self, task_id: int) -> None:
        task = await self._load_task(task_id)
        if not task:
            await self.log.push(f"copy task #{task_id} not found", "ERROR", "copytrade")
            return

        listener_task_id = task["listener_task_id"]
        if not listener_task_id:
            await self.log.push(
                f"copy task #{task_id}: no listener linked", "ERROR", "copytrade",
            )
            return

        wallet = await self._load_wallet(task["wallet_id"])
        if not wallet:
            await self.log.push(
                f"copy task #{task_id}: wallet #{task['wallet_id']} not found",
                "ERROR", "copytrade",
            )
            return

        buy_config = json.loads(task.get("buy_config") or "{}")
        sell_config = json.loads(task.get("sell_config") or "{}")

        await self._set_status(task_id, TASK_STATUS_RUNNING)
        await self.log.push(
            f"copy task #{task_id} started — listening on #{listener_task_id}, "
            f"wallet #{task['wallet_id']}, buy={task['buy_mode']}, sell={task['sell_mode']}",
            "INFO", "copytrade",
        )

        event_queue: asyncio.Queue = asyncio.Queue()

        async def on_event(_listener_task_id: int, trade_event):
            await event_queue.put(trade_event)

        self.listener.register_copy_callback(listener_task_id, on_event)
        monitor_task = None

        try:
            # Start TP/SL monitor if sell mode includes tp_sl
            if task["sell_mode"] in ("tp_sl", "both") and sell_config:
                monitor_task = asyncio.create_task(
                    self._tpsl_monitor(task_id, task, wallet, sell_config)
                )

            while True:
                event = await event_queue.get()
                try:
                    await self._handle_event(task_id, task, wallet, buy_config, event)
                except Exception as exc:
                    await self.log.push(
                        f"copy #{task_id} error: {exc}", "ERROR", "copytrade",
                    )
                finally:
                    event_queue.task_done()

        except asyncio.CancelledError:
            self.listener.unregister_copy_callback(listener_task_id, on_event)
            if monitor_task and not monitor_task.done():
                monitor_task.cancel()
                try:
                    await monitor_task
                except asyncio.CancelledError:
                    pass
            await self._set_status(task_id, TASK_STATUS_PAUSED)
            await self.log.push(f"copy task #{task_id} paused", "WARNING", "copytrade")
            raise

    # ------------------------------------------------------------------
    # Event handler
    # ------------------------------------------------------------------

    async def _handle_event(self, task_id, task, wallet, buy_config, event) -> None:
        action = event.action  # buy, sell, swap, create, transfer_in, transfer_out
        token_short = event.token[:10] + "..." if event.token else "?"

        await self.log.push(
            f"copy #{task_id}: event received — {action} {token_short} on {event.platform}",
            "INFO", "copytrade",
        )

        if action == "buy":
            await self._do_copy_buy(task_id, task, wallet, buy_config, event)
        elif action in ("sell", "swap"):
            # Follow sell: triggered when sell_mode is copy_sell or both
            if task["sell_mode"] in ("copy_sell", "both"):
                await self._do_copy_sell(task_id, task, wallet, event)
            else:
                await self.log.push(
                    f"copy #{task_id}: skip {action} — sell_mode is '{task['sell_mode']}'",
                    "INFO", "copytrade",
                )
        else:
            await self.log.push(
                f"copy #{task_id}: ignored event action '{action}'",
                "INFO", "copytrade",
            )

    # ------------------------------------------------------------------
    # Copy buy
    # ------------------------------------------------------------------

    async def _do_copy_buy(self, task_id, task, wallet, buy_config, event) -> None:
        target_bnb = event.amount_bnb or 0
        buy_mode = task["buy_mode"]

        if buy_mode == "fixed":
            amount_bnb = buy_config.get("amount", 0)
        elif buy_mode == "smart":
            conditions = buy_config.get("conditions", [])
            # Sort by min_bnb descending — pick highest matching tier
            conditions.sort(key=lambda c: c.get("min_bnb", 0), reverse=True)
            amount_bnb = 0
            for cond in conditions:
                if target_bnb >= cond.get("min_bnb", 0):
                    amount_bnb = cond.get("amount", 0)
                    break
        else:
            return

        if amount_bnb <= 0:
            return  # below all smart thresholds or zero config

        amount_wei = int(amount_bnb * 1e18)
        token = event.token
        platform = event.platform

        if not token or token == "UNKNOWN":
            await self.log.push(
                f"copy #{task_id}: skip buy — unknown token", "WARNING", "copytrade",
            )
            return

        # Retry loop with escalating slippage: 5→10→10→15→20
        for attempt in range(1, BUY_MAX_RETRIES + 1):
            slippage = BUY_SLIPPAGE_TIERS[attempt - 1]
            try:
                await self.log.push(
                    f"copy #{task_id}: BUY attempt {attempt}/{BUY_MAX_RETRIES} "
                    f"{amount_bnb:.4f} BNB → {token[:10]}... on {platform} (slippage {slippage}%)",
                    "INFO", "copytrade",
                )

                result = await self.router.buy(
                    platform=platform,
                    token=token,
                    amount_bnb_wei=amount_wei,
                    private_key=wallet["private_key"],
                    slippage=slippage,
                    gas_multiplier=task["gas_multiplier"],
                    chain=task.get("chain", "bsc"),
                )

                tx_hash = result["tx_hash"]
                estimated_tokens = result.get("estimated_tokens", "0")

                # Record position
                await self._open_position(
                    task_id, token, platform, amount_bnb, estimated_tokens, tx_hash,
                )

                # Record transaction
                await self._record_tx(
                    tx_hash=tx_hash,
                    task=task,
                    action="buy",
                    token=token,
                    amount=amount_bnb,
                    amount_token=estimated_tokens,
                    platform=platform,
                    chain=task.get("chain", "bsc"),
                )

                await self.log.push(
                    f"copy #{task_id}: BUY sent {amount_bnb:.4f} BNB → "
                    f"{token[:10]}... | {tx_hash[:16]}...",
                    "SUCCESS", "copytrade", tx_hash=tx_hash,
                )
                return  # success — exit retry loop

            except Exception as exc:
                if attempt < BUY_MAX_RETRIES:
                    delay = RETRY_BASE_DELAY * attempt
                    await self.log.push(
                        f"copy #{task_id}: BUY attempt {attempt} failed — {exc}, "
                        f"retrying in {delay}s...",
                        "WARNING", "copytrade",
                    )
                    await asyncio.sleep(delay)
                else:
                    await self.log.push(
                        f"copy #{task_id}: BUY failed after {BUY_MAX_RETRIES} attempts — {exc}",
                        "ERROR", "copytrade",
                    )

    # ------------------------------------------------------------------
    # Copy sell
    # ------------------------------------------------------------------

    async def _do_copy_sell(self, task_id, task, wallet, event) -> None:
        token = event.token

        if not token or token == "UNKNOWN":
            await self.log.push(
                f"copy #{task_id}: skip SELL — unknown token", "WARNING", "copytrade",
            )
            return

        chain = task.get("chain", "bsc")
        wallet_addr = wallet["address"]

        # Use the platform we BOUGHT on (from our position), not the target's platform
        positions = await self._open_positions_for_token(task_id, token)
        if not positions:
            await self.log.push(
                f"copy #{task_id}: skip SELL {token[:10]}... — no open position for this token",
                "WARNING", "copytrade",
            )
            return

        sell_platform = positions[0]["platform"]

        # Retry until success
        for attempt in range(1, SELL_MAX_RETRIES + 1):
            # Re-fetch balance each attempt (might change after failed tx)
            try:
                balance = await self.router.get_token_balance(token, wallet_addr, chain)
            except Exception:
                balance = 0

            if balance == 0:
                await self.log.push(
                    f"copy #{task_id}: SELL {token[:10]}... — zero balance, nothing to sell",
                    "WARNING", "copytrade",
                )
                return

            # Try buy platform first, then PCS fallback
            platforms_to_try = [sell_platform]
            if sell_platform != "dex":
                platforms_to_try.append("dex")

            result = None
            used_platform = sell_platform
            last_err = None

            for plat in platforms_to_try:
                try:
                    await self.log.push(
                        f"copy #{task_id}: SELL attempt {attempt}/{SELL_MAX_RETRIES} "
                        f"{token[:10]}... on {plat}",
                        "INFO", "copytrade",
                    )
                    sell_slippage = SELL_SLIPPAGE_TIERS[min(attempt - 1, len(SELL_SLIPPAGE_TIERS) - 1)]
                    result = await self.router.sell(
                        platform=plat,
                        token=token,
                        amount_token_raw=balance,
                        private_key=wallet["private_key"],
                        slippage=sell_slippage,
                        gas_multiplier=task["gas_multiplier"],
                        chain=chain,
                    )
                    used_platform = plat
                    break  # success
                except Exception as exc:
                    last_err = exc
                    if plat != platforms_to_try[-1]:
                        await self.log.push(
                            f"copy #{task_id}: SELL on {plat} failed ({exc}), trying next...",
                            "WARNING", "copytrade",
                        )

            if result:
                # Success — record and return
                tx_hash = result["tx_hash"]
                bnb_out = result.get("estimated_bnb", 0)

                entry_bnb = await self._close_positions(
                    task_id, token, tx_hash,
                    sell_bnb=bnb_out, reason="follow",
                )

                extra = {"reason": "follow"}
                if bnb_out and entry_bnb > 0:
                    extra["profit_bnb"] = round(bnb_out - entry_bnb, 6)
                    extra["profit_pct"] = round((bnb_out - entry_bnb) / entry_bnb * 100, 2)

                await self._record_tx(
                    tx_hash=tx_hash,
                    task=task,
                    action="sell",
                    token=token,
                    amount=bnb_out,
                    amount_token=str(balance),
                    platform=used_platform,
                    chain=chain,
                    extra=json.dumps(extra),
                )

                await self.log.push(
                    f"copy #{task_id}: SELL sent {token[:10]}... "
                    f"~{bnb_out:.4f} BNB on {used_platform} | {tx_hash[:16]}...",
                    "SUCCESS", "copytrade", tx_hash=tx_hash,
                )
                return  # done

            # All platforms failed this attempt — retry
            delay = min(RETRY_BASE_DELAY * attempt, 15)
            await self.log.push(
                f"copy #{task_id}: SELL attempt {attempt} failed — {last_err}, "
                f"retrying in {delay}s...",
                "WARNING", "copytrade",
            )
            await asyncio.sleep(delay)

        await self.log.push(
            f"copy #{task_id}: SELL {token[:10]}... failed after {SELL_MAX_RETRIES} attempts",
            "ERROR", "copytrade",
        )

    # ------------------------------------------------------------------
    # TP/SL price monitor
    # ------------------------------------------------------------------

    async def _tpsl_monitor(self, task_id, task, wallet, sell_config) -> None:
        tp_pct = sell_config.get("take_profit", 50)
        sl_pct = sell_config.get("stop_loss", 20)
        chain = task.get("chain", "bsc")

        await self.log.push(
            f"copy #{task_id}: TP/SL monitor started (TP={tp_pct}%, SL={sl_pct}%)",
            "INFO", "copytrade",
        )

        while True:
            await asyncio.sleep(TPSL_CHECK_INTERVAL)
            try:
                positions = await self._open_positions(task_id)

                # Group positions by token to avoid double-counting
                tokens_seen: dict[str, dict] = {}
                for pos in positions:
                    tk = pos["token"].lower()
                    if tk not in tokens_seen:
                        tokens_seen[tk] = {
                            "token": pos["token"],
                            "platform": pos["platform"],
                            "total_entry_bnb": 0.0,
                        }
                    tokens_seen[tk]["total_entry_bnb"] += pos["amount_bnb"] or 0

                for tk, info in tokens_seen.items():
                    token = info["token"]
                    total_entry_bnb = info["total_entry_bnb"]
                    if total_entry_bnb <= 0:
                        continue

                    # Get current on-chain balance (once per token)
                    balance = await self.router.get_token_balance(
                        token, wallet["address"], chain,
                    )
                    if balance == 0:
                        await self._close_positions(task_id, token, "balance_zero")
                        continue

                    # Estimate current BNB value
                    current_bnb = await self.router.get_sell_value_bnb(
                        info["platform"], token, balance, chain,
                    )
                    if current_bnb <= 0:
                        continue

                    pnl_pct = (current_bnb - total_entry_bnb) / total_entry_bnb * 100

                    # Check thresholds
                    if pnl_pct >= tp_pct:
                        await self.log.push(
                            f"copy #{task_id}: TP triggered "
                            f"{token[:10]}... +{pnl_pct:.1f}%",
                            "SUCCESS", "copytrade",
                        )
                        # Pass first position (sell uses full balance anyway)
                        await self._execute_tpsl_sell(
                            task_id, task, wallet,
                            {"token": token, "platform": info["platform"],
                             "amount_bnb": total_entry_bnb},
                            balance, "take_profit",
                        )
                    elif pnl_pct <= -sl_pct:
                        await self.log.push(
                            f"copy #{task_id}: SL triggered "
                            f"{token[:10]}... {pnl_pct:.1f}%",
                            "WARNING", "copytrade",
                        )
                        await self._execute_tpsl_sell(
                            task_id, task, wallet,
                            {"token": token, "platform": info["platform"],
                             "amount_bnb": total_entry_bnb},
                            balance, "stop_loss",
                        )

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("tpsl monitor error (task %d): %s", task_id, exc)

    async def _execute_tpsl_sell(
        self, task_id, task, wallet, pos, balance, reason,
    ) -> None:
        token = pos["token"]
        chain = task.get("chain", "bsc")
        sell_platform = pos["platform"]

        platforms_to_try = [sell_platform]
        if sell_platform != "dex":
            platforms_to_try.append("dex")

        for attempt in range(1, SELL_MAX_RETRIES + 1):
            # Re-fetch balance each attempt
            try:
                balance = await self.router.get_token_balance(
                    token, wallet["address"], chain,
                )
            except Exception:
                pass
            if balance == 0:
                await self.log.push(
                    f"copy #{task_id}: {reason} SELL {token[:10]}... — zero balance",
                    "WARNING", "copytrade",
                )
                return

            result = None
            used_platform = sell_platform
            last_err = None

            for plat in platforms_to_try:
                try:
                    sell_slippage = SELL_SLIPPAGE_TIERS[min(attempt - 1, len(SELL_SLIPPAGE_TIERS) - 1)]
                    result = await self.router.sell(
                        platform=plat,
                        token=token,
                        amount_token_raw=balance,
                        private_key=wallet["private_key"],
                        slippage=sell_slippage,
                        gas_multiplier=task["gas_multiplier"],
                        chain=chain,
                    )
                    used_platform = plat
                    break
                except Exception as exc:
                    last_err = exc

            if result:
                tx_hash = result["tx_hash"]
                bnb_out = result.get("estimated_bnb", 0)
                entry_bnb = await self._close_positions(
                    task_id, token, tx_hash,
                    sell_bnb=bnb_out, reason=reason,
                )

                extra = {"reason": reason}
                if bnb_out and entry_bnb > 0:
                    extra["profit_bnb"] = round(bnb_out - entry_bnb, 6)
                    extra["profit_pct"] = round((bnb_out - entry_bnb) / entry_bnb * 100, 2)

                await self._record_tx(
                    tx_hash=tx_hash,
                    task=task,
                    action="sell",
                    token=token,
                    amount=bnb_out,
                    amount_token=str(balance),
                    platform=used_platform,
                    chain=chain,
                    extra=json.dumps(extra),
                )
                await self.log.push(
                    f"copy #{task_id}: {reason} SELL {token[:10]}... "
                    f"~{bnb_out:.4f} BNB | {tx_hash[:16]}...",
                    "SUCCESS", "copytrade", tx_hash=tx_hash,
                )
                return  # done

            delay = min(RETRY_BASE_DELAY * attempt, 15)
            await self.log.push(
                f"copy #{task_id}: {reason} sell attempt {attempt} failed — {last_err}, "
                f"retrying in {delay}s...",
                "WARNING", "copytrade",
            )
            await asyncio.sleep(delay)

        await self.log.push(
            f"copy #{task_id}: {reason} SELL {token[:10]}... failed after {SELL_MAX_RETRIES} attempts",
            "ERROR", "copytrade",
        )

    # ------------------------------------------------------------------
    # Manual sell (user-initiated from UI)
    # ------------------------------------------------------------------

    async def manual_sell(self, position_id: int) -> dict:
        """Sell an open position manually. Returns {tx_hash, bnb_out} or raises."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """SELECT cp.*, ct.wallet_id, ct.gas_multiplier, ct.id AS task_id,
                          lt.chain
                   FROM copy_positions cp
                   JOIN copy_tasks ct ON cp.copy_task_id = ct.id
                   LEFT JOIN listener_tasks lt ON ct.listener_task_id = lt.id
                   WHERE cp.id = ? AND cp.status = 'open'""",
                (position_id,),
            )
            pos = await cur.fetchone()
            if not pos:
                raise ValueError("position not found or already closed")
            pos = dict(pos)

        wallet = await self._load_wallet(pos["wallet_id"])
        if not wallet:
            raise ValueError("wallet not found")

        chain = pos.get("chain") or "bsc"
        token = pos["token"]
        platform = pos["platform"]

        balance = await self.router.get_token_balance(token, wallet["address"], chain)
        if balance == 0:
            # Close position with zero sell if no balance
            await self._close_positions(
                pos["task_id"], token, "manual_zero_balance",
                sell_bnb=0, reason="manual",
            )
            return {"tx_hash": "", "bnb_out": 0, "message": "zero balance, position closed"}

        result = await self.router.sell(
            platform=platform,
            token=token,
            amount_token_raw=balance,
            private_key=wallet["private_key"],
            slippage=SELL_SLIPPAGE_TIERS[0],
            gas_multiplier=pos.get("gas_multiplier", 1.2),
            chain=chain,
        )

        tx_hash = result["tx_hash"]
        bnb_out = result.get("estimated_bnb", 0)

        entry_bnb = await self._close_positions(
            pos["task_id"], token, tx_hash,
            sell_bnb=bnb_out, reason="manual",
        )

        extra = {"reason": "manual"}
        if bnb_out and entry_bnb > 0:
            extra["profit_bnb"] = round(bnb_out - entry_bnb, 6)
            extra["profit_pct"] = round((bnb_out - entry_bnb) / entry_bnb * 100, 2)

        # Load task for _record_tx
        task = await self._load_task(pos["task_id"])
        if task:
            await self._record_tx(
                tx_hash=tx_hash,
                task=task,
                action="sell",
                token=token,
                amount=bnb_out,
                amount_token=str(balance),
                platform=platform,
                chain=chain,
                extra=json.dumps(extra),
            )

        await self.log.push(
            f"copy #{pos['task_id']}: MANUAL SELL {token[:10]}... "
            f"~{bnb_out:.4f} BNB | {tx_hash[:16]}...",
            "SUCCESS", "copytrade", tx_hash=tx_hash,
        )

        return {"tx_hash": tx_hash, "bnb_out": bnb_out}

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    async def _load_task(self, task_id: int) -> dict | None:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """SELECT ct.*, lt.target_address, lt.chain
                   FROM copy_tasks ct
                   JOIN listener_tasks lt ON ct.listener_task_id = lt.id
                   WHERE ct.id = ?""",
                (task_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def _load_wallet(self, wallet_id: int) -> dict | None:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT id, address, private_key FROM wallets WHERE id = ?",
                (wallet_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def _set_status(self, task_id: int, status: str) -> None:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE copy_tasks SET status = ? WHERE id = ?", (status, task_id),
            )
            await db.commit()

    async def _open_position(
        self, task_id, token, platform, amount_bnb, amount_token, tx_hash,
    ) -> None:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT INTO copy_positions
                   (copy_task_id, token, platform, amount_bnb, amount_token, buy_tx_hash)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (task_id, token, platform, amount_bnb, str(amount_token), tx_hash),
            )
            await db.commit()

    async def _open_positions(self, task_id: int) -> list[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM copy_positions WHERE copy_task_id = ? AND status = 'open'",
                (task_id,),
            )
            return [dict(r) for r in await cur.fetchall()]

    async def _open_positions_for_token(self, task_id: int, token: str) -> list[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """SELECT * FROM copy_positions
                   WHERE copy_task_id = ? AND LOWER(token) = LOWER(?) AND status = 'open'""",
                (task_id, token),
            )
            return [dict(r) for r in await cur.fetchall()]

    async def _close_positions(
        self, task_id: int, token: str, tx_hash: str,
        sell_bnb: float = 0, reason: str = "",
    ) -> float:
        """Close open positions. Returns total entry BNB for P&L calculation."""
        total_entry = 0.0
        async with aiosqlite.connect(DB_PATH) as db:
            # Fetch open positions to compute P&L
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """SELECT id, amount_bnb FROM copy_positions
                   WHERE copy_task_id = ? AND LOWER(token) = LOWER(?) AND status = 'open'""",
                (task_id, token),
            )
            rows = [dict(r) for r in await cur.fetchall()]

            for pos in rows:
                total_entry += pos["amount_bnb"] or 0

            for pos in rows:
                entry_bnb = pos["amount_bnb"] or 0
                # Distribute sell_bnb proportionally across positions
                if sell_bnb and total_entry > 0:
                    share = entry_bnb / total_entry
                    pos_sell_bnb = sell_bnb * share
                    profit_bnb = pos_sell_bnb - entry_bnb
                    profit_pct = (profit_bnb / entry_bnb * 100) if entry_bnb > 0 else None
                else:
                    pos_sell_bnb = None
                    profit_bnb = None
                    profit_pct = None
                await db.execute(
                    """UPDATE copy_positions
                       SET status = 'closed', sell_tx_hash = ?,
                           sell_amount_bnb = ?, sell_reason = ?,
                           profit_bnb = ?, profit_pct = ?,
                           sold_at = CURRENT_TIMESTAMP
                       WHERE id = ?""",
                    (tx_hash, pos_sell_bnb, reason or None,
                     profit_bnb, profit_pct, pos["id"]),
                )
            await db.commit()
        return total_entry

    async def _record_tx(
        self, tx_hash, task, action, token, amount, amount_token,
        platform, chain, extra="{}",
    ) -> None:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """INSERT INTO transactions
                   (tx_hash, target_address, source_task_id, source_task_type,
                    action, token, pair, amount, amount_token, status, platform, chain, extra)
                   VALUES (?, ?, ?, 'copy', ?, ?, ?, ?, ?, 'submitted', ?, ?, ?)""",
                (
                    tx_hash,
                    task.get("target_address", ""),
                    task["id"],
                    action,
                    token,
                    f"{token[:10]}/BNB",
                    amount,
                    str(amount_token),
                    platform,
                    chain,
                    extra,
                ),
            )
            await db.commit()
