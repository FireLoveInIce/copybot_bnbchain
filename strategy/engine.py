"""Strategy engine — take-profit / stop-loss monitor.

For each running strategy task the engine periodically fetches the token's
current price from the PancakeSwap V2 pair and executes a sell when the
configured TP or SL threshold is crossed.

Price is expressed in BNB per token unit.
"""

from __future__ import annotations

import asyncio
import logging
import time

import aiosqlite
from eth_account import Account
from web3 import AsyncWeb3

from core.constants import (
    PANCAKESWAP_ROUTER,
    STRATEGY_CHECK_INTERVAL,
    TASK_STATUS_PAUSED,
    TASK_STATUS_RUNNING,
    WBNB_ADDRESS,
)
from database.db import DB_PATH
from logs.service import LogService
from rpc.manager import RpcManager

logger = logging.getLogger(__name__)

# Minimal ABIs
_FACTORY_ABI = [
    {
        "name": "getPair",
        "type": "function",
        "inputs": [
            {"name": "tokenA", "type": "address"},
            {"name": "tokenB", "type": "address"},
        ],
        "outputs": [{"name": "pair", "type": "address"}],
    }
]
_PAIR_ABI = [
    {
        "name": "getReserves",
        "type": "function",
        "inputs": [],
        "outputs": [
            {"name": "_reserve0", "type": "uint112"},
            {"name": "_reserve1", "type": "uint112"},
            {"name": "_blockTimestampLast", "type": "uint32"},
        ],
    },
    {
        "name": "token0",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "address"}],
    },
]
_ROUTER_ABI = [
    {
        "name": "swapExactTokensForETH",
        "type": "function",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
    }
]
_ERC20_ABI = [
    {
        "name": "balanceOf",
        "type": "function",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "approve",
        "type": "function",
        "inputs": [
            {"name": "spender", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
]

PANCAKESWAP_FACTORY = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"


class StrategyEngine:
    def __init__(self, log_service: LogService, rpc_manager: RpcManager):
        self.log = log_service
        self.rpc = rpc_manager
        # entry_price_bnb[task_id] = price at which the position was entered
        self._entry_prices: dict[int, float] = {}

    async def run_strategy_task(self, task_id: int) -> None:
        task = await self._load_task(task_id)
        if not task:
            await self.log.push(
                f"strategy #{task_id} not found", "ERROR", "strategy"
            )
            return

        await self._set_status(task_id, TASK_STATUS_RUNNING)
        await self.log.push(
            f"strategy #{task_id} started — TP={task['take_profit']}% SL={task['stop_loss']}%",
            "INFO",
            "strategy",
        )

        try:
            await self._monitor_loop(task_id, task)
        except asyncio.CancelledError:
            await self._set_status(task_id, TASK_STATUS_PAUSED)
            await self.log.push(
                f"strategy #{task_id} paused", "WARNING", "strategy"
            )
            raise

    async def _monitor_loop(self, task_id: int, task: dict) -> None:
        w3 = await self.rpc.get_http()
        token_addr = AsyncWeb3.to_checksum_address(task["token"])
        wbnb = AsyncWeb3.to_checksum_address(WBNB_ADDRESS)

        entry = self._entry_prices.get(task_id)

        while True:
            try:
                price = await self._get_price_bnb(w3, token_addr, wbnb)
                if price is None:
                    await asyncio.sleep(STRATEGY_CHECK_INTERVAL)
                    continue

                if entry is None:
                    entry = price
                    self._entry_prices[task_id] = entry

                pct_change = (price - entry) / entry * 100

                tp = task.get("take_profit")
                sl = task.get("stop_loss")

                if tp is not None and pct_change >= tp:
                    await self.log.push(
                        f"strategy #{task_id} TP hit +{pct_change:.1f}% — selling",
                        "SUCCESS",
                        "strategy",
                    )
                    await self._execute_sell(task, w3, token_addr, wbnb, slippage=5)
                    await self._set_status(task_id, "completed")
                    break

                if sl is not None and pct_change <= -abs(sl):
                    await self.log.push(
                        f"strategy #{task_id} SL hit {pct_change:.1f}% — selling",
                        "WARNING",
                        "strategy",
                    )
                    await self._execute_sell(task, w3, token_addr, wbnb, slippage=10)
                    await self._set_status(task_id, "completed")
                    break

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self.log.push(
                    f"strategy #{task_id} error: {exc}", "ERROR", "strategy"
                )

            await asyncio.sleep(STRATEGY_CHECK_INTERVAL)

    async def _get_price_bnb(
        self, w3: AsyncWeb3, token: str, wbnb: str
    ) -> float | None:
        """Return token price in BNB using PancakeSwap V2 reserves."""
        try:
            factory = w3.eth.contract(
                address=AsyncWeb3.to_checksum_address(PANCAKESWAP_FACTORY),
                abi=_FACTORY_ABI,
            )
            pair_addr = await factory.functions.getPair(token, wbnb).call()
            zero = "0x" + "0" * 40
            if pair_addr.lower() == zero:
                return None

            pair = w3.eth.contract(
                address=AsyncWeb3.to_checksum_address(pair_addr), abi=_PAIR_ABI
            )
            reserves = await pair.functions.getReserves().call()
            token0 = await pair.functions.token0().call()

            r0, r1 = reserves[0], reserves[1]
            if token0.lower() == token.lower():
                # token is token0, WBNB is token1
                return r1 / r0
            else:
                return r0 / r1
        except Exception:
            return None

    async def _execute_sell(
        self,
        task: dict,
        w3: AsyncWeb3,
        token: str,
        wbnb: str,
        slippage: int = 5,
    ) -> None:
        wallet = await self._load_wallet(task["wallet_id"])
        if not wallet:
            return
        account = Account.from_key(wallet["private_key"])

        erc20 = w3.eth.contract(address=token, abi=_ERC20_ABI)
        balance: int = await erc20.functions.balanceOf(account.address).call()
        if balance == 0:
            return

        router = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(PANCAKESWAP_ROUTER), abi=_ROUTER_ABI
        )
        nonce = await w3.eth.get_transaction_count(account.address)
        gas_price = await w3.eth.gas_price
        deadline = int(time.time()) + 300

        approve_tx = await erc20.functions.approve(
            AsyncWeb3.to_checksum_address(PANCAKESWAP_ROUTER), balance
        ).build_transaction(
            {"from": account.address, "nonce": nonce, "gasPrice": gas_price}
        )
        signed_approve = account.sign_transaction(approve_tx)
        await w3.eth.send_rawTransaction(signed_approve.rawTransaction)

        swap_tx = await router.functions.swapExactTokensForETH(
            balance, 0, [token, wbnb], account.address, deadline
        ).build_transaction(
            {"from": account.address, "nonce": nonce + 1, "gasPrice": gas_price}
        )
        signed_swap = account.sign_transaction(swap_tx)
        tx_hash = await w3.eth.send_rawTransaction(signed_swap.rawTransaction)

        await self.log.push(
            f"strategy sell executed: {tx_hash.hex()}",
            "SUCCESS",
            "strategy",
            tx_hash=tx_hash.hex(),
        )

    async def _load_task(self, task_id: int) -> dict | None:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM strategy_tasks WHERE id = ?", (task_id,)
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def _load_wallet(self, wallet_id: int) -> dict | None:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT address, private_key FROM wallets WHERE id = ?", (wallet_id,)
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def _set_status(self, task_id: int, status: str) -> None:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE strategy_tasks SET status = ? WHERE id = ?",
                (status, task_id),
            )
            await db.commit()
