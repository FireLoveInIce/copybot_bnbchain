"""Copy-trade engine.

Consumes trade events from the listener queue and executes mirror trades
using the configured wallet for each target address.

Buy logic
---------
  fixed : buy exactly `buy_value` BNB worth of the token
  ratio : buy `buy_value * target_bnb_amount` BNB worth of the token

Sell logic
----------
  mirror  : sell when target sells (same percentage if determinable)
  custom  : sell is handled by the strategy engine (TP/SL); this engine skips sells
"""

from __future__ import annotations

import asyncio
import logging

import aiosqlite
from eth_account import Account
from web3 import AsyncWeb3

from core.constants import PLATFORM_CONTRACTS, TASK_STATUS_RUNNING
from database.db import DB_PATH
from logs.service import LogService
from rpc.manager import RpcManager

logger = logging.getLogger(__name__)


class CopyTradeEngine:
    def __init__(
        self,
        log_service: LogService,
        rpc_manager: RpcManager,
        queue: asyncio.Queue,
    ):
        self.log = log_service
        self.rpc = rpc_manager
        self.queue = queue

    async def run(self) -> None:
        """Consume events from the queue indefinitely."""
        await self.log.push("copy-trade engine started", "INFO", "copytrade")
        try:
            while True:
                event = await self.queue.get()
                try:
                    await self._handle(event)
                except Exception as exc:
                    await self.log.push(
                        f"copy-trade error: {exc}", "ERROR", "copytrade"
                    )
                finally:
                    self.queue.task_done()
        except asyncio.CancelledError:
            await self.log.push("copy-trade engine stopped", "WARNING", "copytrade")
            raise

    # ------------------------------------------------------------------
    # Core logic
    # ------------------------------------------------------------------

    async def _handle(self, event: dict) -> None:
        target = event["target_address"]
        action = event["action"]
        chain = event.get("chain", "bsc")

        copy_tasks = await self._active_copy_tasks(target)
        if not copy_tasks:
            return

        for task in copy_tasks:
            try:
                if action == "buy":
                    await self._execute_buy(task, event, chain)
                elif action == "sell" and task["sell_mode"] == "mirror":
                    await self._execute_sell(task, event, chain)
                # sell + custom → handled by strategy engine
            except Exception as exc:
                await self.log.push(
                    f"copy task #{task['id']} failed: {exc}", "ERROR", "copytrade"
                )

    async def _execute_buy(self, task: dict, event: dict, chain: str) -> None:
        wallet = await self._load_wallet(task["wallet_id"])
        if not wallet:
            return

        contract_addr = PLATFORM_CONTRACTS.get(event["platform"], "")
        if not contract_addr:
            await self.log.push(
                f"copy task #{task['id']}: platform contract not configured",
                "WARNING",
                "copytrade",
            )
            return

        target_amount_wei: int = event.get("amount_wei", 0)
        if task["buy_mode"] == "fixed":
            spend_wei = int(task["buy_value"] * 1e18)
        else:  # ratio
            spend_wei = int(target_amount_wei * task["buy_value"])

        if spend_wei == 0:
            return

        w3 = await self.rpc.get_http(chain)
        account = Account.from_key(wallet["private_key"])
        gas_price = int(await w3.eth.gas_price * task["gas_multiplier"])
        nonce = await w3.eth.get_transaction_count(account.address)

        # Replay the original transaction input so the platform contract
        # receives the same call with our adjusted BNB value.
        raw_tx = {
            "to": AsyncWeb3.to_checksum_address(contract_addr),
            "value": spend_wei,
            "data": event.get("raw_input", "0x"),
            "gas": 300_000,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": 56,
        }

        signed = account.sign_transaction(raw_tx)
        tx_hash = await w3.eth.send_rawTransaction(signed.rawTransaction)

        await self._record_tx(
            tx_hash=tx_hash.hex(),
            task=task,
            action="buy",
            token=event.get("token", ""),
            amount=spend_wei / 1e18,
            platform=event["platform"],
            chain=chain,
        )
        await self.log.push(
            f"copy buy sent: {spend_wei/1e18:.4f} BNB → {event.get('token','')} | {tx_hash.hex()}",
            "SUCCESS",
            "copytrade",
            tx_hash=tx_hash.hex(),
        )

    async def _execute_sell(self, task: dict, event: dict, chain: str) -> None:
        """Mirror sell — calls the same platform contract sell function."""
        wallet = await self._load_wallet(task["wallet_id"])
        if not wallet:
            return

        contract_addr = PLATFORM_CONTRACTS.get(event["platform"], "")
        if not contract_addr:
            return

        w3 = await self.rpc.get_http(chain)
        account = Account.from_key(wallet["private_key"])
        gas_price = int(await w3.eth.gas_price * task["gas_multiplier"])
        nonce = await w3.eth.get_transaction_count(account.address)

        raw_tx = {
            "to": AsyncWeb3.to_checksum_address(contract_addr),
            "value": 0,
            "data": event.get("raw_input", "0x"),
            "gas": 300_000,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": 56,
        }

        signed = account.sign_transaction(raw_tx)
        tx_hash = await w3.eth.send_rawTransaction(signed.rawTransaction)

        await self._record_tx(
            tx_hash=tx_hash.hex(),
            task=task,
            action="sell",
            token=event.get("token", ""),
            amount=0,
            platform=event["platform"],
            chain=chain,
        )
        await self.log.push(
            f"copy sell sent: {event.get('token','')} | {tx_hash.hex()}",
            "SUCCESS",
            "copytrade",
            tx_hash=tx_hash.hex(),
        )

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    async def _active_copy_tasks(self, target_address: str) -> list[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT * FROM copy_tasks
                WHERE target_address = ? AND status = ?
                """,
                (target_address, TASK_STATUS_RUNNING),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def _load_wallet(self, wallet_id: int) -> dict | None:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT address, private_key FROM wallets WHERE id = ?",
                (wallet_id,),
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def _record_tx(
        self,
        tx_hash: str,
        task: dict,
        action: str,
        token: str,
        amount: float,
        platform: str,
        chain: str,
    ) -> None:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                """
                INSERT INTO transactions
                (tx_hash, target_address, source_task_id, source_task_type,
                 action, token, pair, amount, status, platform, chain)
                VALUES (?, ?, ?, 'copy', ?, ?, ?, ?, 'submitted', ?, ?)
                """,
                (
                    tx_hash,
                    task["target_address"],
                    task["id"],
                    action,
                    token,
                    f"{token}/BNB",
                    amount,
                    platform,
                    chain,
                ),
            )
            await db.commit()
