"""Listener engine — shared block stream architecture.

One BlockStream per chain polls for new blocks via HTTP, fetches each
block ONCE, then dispatches matching txs to all active listener tasks.
Block cursor only advances on success — infinite retry with backoff
guarantees no blocks are ever skipped.
"""

from __future__ import annotations

import asyncio
import json
import logging

import aiosqlite

from core.constants import (
    LISTENER_POLL_INTERVAL,
    TASK_STATUS_PAUSED,
    TASK_STATUS_RUNNING,
)
from database.db import DB_PATH
from listener.decoder import (
    ReceiptDecoder,
    TradeEvent,
)
from logs.service import LogService
from rpc.manager import RpcManager

logger = logging.getLogger(__name__)


# ======================================================================
# Shared block stream — one per chain (HTTP poll-only)
# ======================================================================

class BlockStream:
    """Single HTTP poll loop that fetches each block once and fans out.

    The block cursor only advances after a block is successfully fetched
    and processed.  If a fetch fails, it retries with exponential backoff
    until it succeeds — guaranteeing zero missed blocks.
    """

    def __init__(self, chain: str, rpc: RpcManager, log: LogService):
        self.chain = chain
        self.rpc = rpc
        self.log = log
        # target_address (lower) → set of (task_id, task_id_hash, callback)
        self._subscribers: dict[str, set[tuple]] = {}
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def subscribe(self, target: str, task_id: int, task: dict, callback):
        """Register a listener task. Starts the stream if not running."""
        key = target.lower()
        async with self._lock:
            if key not in self._subscribers:
                self._subscribers[key] = set()
            self._subscribers[key].add((task_id, id(task), callback))
            if self._task is None or self._task.done():
                self._task = asyncio.create_task(self._run())

    async def unsubscribe(self, target: str, task_id: int):
        """Remove a listener task. Stops the stream if no subscribers left."""
        key = target.lower()
        async with self._lock:
            subs = self._subscribers.get(key)
            if subs:
                subs = {s for s in subs if s[0] != task_id}
                if subs:
                    self._subscribers[key] = subs
                else:
                    del self._subscribers[key]
            # Stop stream if empty
            if not self._subscribers and self._task and not self._task.done():
                self._task.cancel()
                self._task = None

    def _get_all_targets(self) -> set[str]:
        return set(self._subscribers.keys())

    async def _dispatch(self, target: str, task_id: int, callback, tx_hash: str):
        try:
            await callback(tx_hash)
        except Exception as exc:
            logger.debug("dispatch error (task %d, tx %s): %s", task_id, tx_hash[:16], exc)

    async def _fan_out(self, block) -> None:
        """Scan one block and dispatch matching txs to subscribers."""
        targets = self._get_all_targets()
        if not targets:
            return

        for tx in block.get("transactions") or []:
            tx_from = str(tx.get("from", "")).lower()
            if tx_from not in targets:
                continue
            h = tx.get("hash")
            tx_hash = h.hex() if hasattr(h, "hex") else str(h)
            subs = self._subscribers.get(tx_from, set())
            for task_id, _tid, callback in list(subs):
                await self._dispatch(tx_from, task_id, callback, tx_hash)

    async def _run(self) -> None:
        """Main poll loop with block cursor — never skips a block."""
        cursor: int | None = None   # last successfully processed block
        retry_delay = 1.0

        await self.log.push(
            f"block stream ({self.chain}) started (HTTP poll, {LISTENER_POLL_INTERVAL}s)",
            "INFO", "listener",
        )

        while self._subscribers:
            try:
                w3 = await self.rpc.get_http(self.chain)
                head = await w3.eth.block_number

                # First run: start from current block (don't replay history)
                if cursor is None:
                    cursor = head - 1

                # Process blocks one by one; stop on failure (don't advance cursor)
                while cursor < head and self._subscribers:
                    next_bn = cursor + 1
                    block = await w3.eth.get_block(next_bn, full_transactions=True)
                    await self._fan_out(block)
                    cursor = next_bn  # only advance after success
                    retry_delay = 1.0  # reset backoff on success

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # Block fetch or RPC error — retry with backoff, never skip
                logger.debug("block stream poll error: %s", exc)
                if retry_delay >= 8:
                    await self.log.push(
                        f"block stream ({self.chain}) retrying block {(cursor or 0) + 1} "
                        f"in {retry_delay:.0f}s: {exc}",
                        "WARNING", "listener",
                    )
                await asyncio.sleep(retry_delay)
                retry_delay = min(retry_delay * 2, 30)
                continue  # skip the normal poll sleep, retry immediately

            await asyncio.sleep(LISTENER_POLL_INTERVAL)


# ======================================================================
# Listener engine
# ======================================================================

class ListenerEngine:
    def __init__(self, log_service: LogService, rpc_manager: RpcManager):
        self.log = log_service
        self.rpc = rpc_manager
        self._decoder = ReceiptDecoder()
        self._streams: dict[str, BlockStream] = {}  # chain → BlockStream
        self._processed: dict[int, set[str]] = {}    # task_id → processed tx hashes

    def _get_stream(self, chain: str) -> BlockStream:
        if chain not in self._streams:
            self._streams[chain] = BlockStream(chain, self.rpc, self.log)
        return self._streams[chain]

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def run_listener_task(self, task_id: int) -> None:
        task = await self._load_task(task_id)
        if not task:
            await self.log.push(f"listener task #{task_id} not found", "ERROR", "listener")
            return

        target = task["target_address"].lower()
        chain = task["chain"]

        await self._set_status(task_id, TASK_STATUS_RUNNING)
        await self.log.push(
            f"listener #{task_id} started — watching {target}",
            "INFO", "listener",
        )

        processed = set()
        self._processed[task_id] = processed
        PROCESSED_MAX = 2000

        async def on_tx(tx_hash: str):
            if tx_hash in processed:
                return
            processed.add(tx_hash)
            if len(processed) > PROCESSED_MAX:
                to_keep = list(processed)[PROCESSED_MAX // 2:]
                processed.clear()
                processed.update(to_keep)
            await self._process_tx(task_id, task, target, tx_hash)

        stream = self._get_stream(chain)
        try:
            await stream.subscribe(target, task_id, task, on_tx)
            # Keep alive until cancelled
            while True:
                await asyncio.sleep(60)
        except asyncio.CancelledError:
            await stream.unsubscribe(target, task_id)
            self._processed.pop(task_id, None)
            await self._set_status(task_id, TASK_STATUS_PAUSED)
            await self.log.push(f"listener #{task_id} paused", "WARNING", "listener")
            raise

    # ------------------------------------------------------------------
    # Process a single transaction
    # ------------------------------------------------------------------

    async def _process_tx(
        self,
        task_id: int,
        task: dict,
        target: str,
        tx_hash: str,
    ) -> None:
        """Fetch full receipt, decode all logs, persist and log results."""
        try:
            w3 = await self.rpc.get_http(task["chain"])
            receipt = await w3.eth.get_transaction_receipt(tx_hash)
            if not receipt or receipt.get("status") == 0:
                return

            tx = await w3.eth.get_transaction(tx_hash)
            tx_from = str(tx.get("from", "")).lower() if tx else ""
            tx_value = int(tx.get("value", 0)) if tx else 0

            # Normalise logs to plain dicts with hex strings
            logs = _normalise_receipt_logs(receipt.get("logs") or [])
            block_number = receipt.get("blockNumber")
            if block_number is not None and not isinstance(block_number, int):
                block_number = int(block_number, 16) if isinstance(block_number, str) else int(block_number)

            events = self._decoder.decode_receipt(
                tx_hash=tx_hash,
                tx_from=tx_from,
                logs=logs,
                target=target,
                block_number=block_number or 0,
                tx_value=tx_value,
            )

            for event in events:
                if event.action in ("transfer_in", "transfer_out") and event.trader:
                    event.extra["counterparty"] = event.trader
                await self._persist_event(task_id, task, event)
                await self._log_event(task_id, event)

        except Exception as exc:
            logger.debug("process_tx error (%s): %s", tx_hash, exc)

    async def _log_event(self, task_id: int, event: TradeEvent) -> None:
        if event.action in ("transfer_in", "transfer_out"):
            direction = "IN" if event.action == "transfer_in" else "OUT"
            counterparty = event.trader or ""
            cp_label = await self._resolve_wallet_label(counterparty)
            if event.action == "transfer_out":
                flow = f"→ {cp_label}"
            else:
                flow = f"← {cp_label}"
            msg = (
                f"[transfer] {direction} {flow} "
                + (f"token={event.token[:10]}..." if event.token else "(no token)")
                + (f"  {event.amount_token / 1e18:.4f} tokens" if event.amount_token else "")
            )
        elif event.action == "swap":
            sold = event.extra.get("sold_tokens", [])
            sold_str = sold[0]["token"][:10] + "..." if sold else "?"
            msg = (
                f"[{event.platform}] SWAP {sold_str}"
                + f" → token={event.token[:10]}..."
                + (f"  {event.amount_token / 1e18:.4f} tokens" if event.amount_token else "")
            )
        else:
            msg = (
                f"[{event.platform}] {event.action.upper()} "
                + (f"token={event.token[:10]}..." if event.token else "(no token)")
                + (f"  {event.amount_bnb:.4f} BNB" if event.amount_bnb else "")
                + (f"  {event.amount_token / 1e18:.4f} tokens" if event.amount_token else "")
            )
        await self.log.push(
            msg, "INFO", "listener",
            tx_hash=event.tx_hash, task_id=task_id,
        )

    async def _resolve_wallet_label(self, address: str) -> str:
        """Return 'WalletLabel#id' if address is a local wallet, else short address."""
        if not address:
            return "unknown"
        addr = address.lower()
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT id, label FROM wallets WHERE LOWER(address) = ?", (addr,)
            )
            row = await cur.fetchone()
        if row:
            name = row["label"] or "Wallet"
            return f"{name}#{row['id']}"
        return f"{addr[:6]}...{addr[-4:]}"

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    async def _load_task(self, task_id: int) -> dict | None:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM listener_tasks WHERE id = ?", (task_id,)
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def _set_status(self, task_id: int, status: str) -> None:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE listener_tasks SET status = ? WHERE id = ?", (status, task_id)
            )
            await db.commit()

    async def _persist_event(
        self, task_id: int, task: dict, event: TradeEvent
    ) -> None:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT OR IGNORE INTO transactions
                (tx_hash, target_address, source_task_id, source_task_type,
                 action, token, pair, amount, amount_token, price,
                 status, platform, chain, extra)
                VALUES (?, ?, ?, 'listener', ?, ?, ?, ?, ?, NULL, 'detected', ?, ?, ?)
                """,
                (
                    event.tx_hash,
                    task["target_address"],
                    task_id,
                    event.action,
                    event.token or "UNKNOWN",
                    f"{event.token[:10] if event.token else 'TOKEN'}/BNB",
                    event.amount_bnb,
                    str(event.amount_token),
                    event.platform,
                    task["chain"],
                    json.dumps(event.extra) if event.extra else "{}",
                ),
            )
            await db.commit()


# ---------------------------------------------------------------------------
# Log normalisation
# ---------------------------------------------------------------------------

def _normalise_receipt_logs(raw_logs: list) -> list[dict]:
    """Convert receipt log entries to plain dicts with hex string values."""
    result = []
    for log in raw_logs:
        d: dict = {}
        d["address"] = str(log.get("address", "")).lower()

        topics = log.get("topics") or []
        d["topics"] = [
            t.hex() if hasattr(t, "hex") else str(t)
            for t in topics
        ]

        data = log.get("data")
        if data is not None:
            d["data"] = data.hex() if hasattr(data, "hex") else str(data)
        else:
            d["data"] = "0x"

        tx_hash = log.get("transactionHash")
        if tx_hash is not None:
            d["transactionHash"] = tx_hash.hex() if hasattr(tx_hash, "hex") else str(tx_hash)

        for key in ("logIndex", "blockNumber"):
            v = log.get(key)
            if v is not None:
                d[key] = hex(v) if isinstance(v, int) else str(v)

        result.append(d)
    return result
