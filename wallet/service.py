"""Wallet service: generation, balances, private-key access, token holdings, transfer, panic-sell."""

from __future__ import annotations

import asyncio
import os
import time
from decimal import Decimal

import aiosqlite
from eth_account import Account
from web3 import AsyncWeb3

from core.constants import DEFAULT_CHAIN, PANCAKESWAP_ROUTER, WBNB_ADDRESS
from database.db import DB_PATH
from rpc.manager import RpcManager

Account.enable_unaudited_hdwallet_features()

_ROUTER_ABI = [
    {
        "name": "getAmountsOut",
        "type": "function",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "path", "type": "address[]"},
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
    },
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
    },
]

_ERC20_ABI = [
    {
        "name": "balanceOf",
        "type": "function",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "decimals",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "uint8"}],
    },
    {
        "name": "symbol",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "string"}],
    },
    {
        "name": "name",
        "type": "function",
        "inputs": [],
        "outputs": [{"name": "", "type": "string"}],
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
    {
        "name": "transfer",
        "type": "function",
        "inputs": [
            {"name": "to", "type": "address"},
            {"name": "amount", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "bool"}],
    },
]


class WalletService:
    def __init__(self, rpc_manager: RpcManager):
        self._rpc = rpc_manager

    def validate_evm_address(self, address: str) -> str:
        """Return checksum address or empty string on failure."""
        try:
            return AsyncWeb3.to_checksum_address(address)
        except (ValueError, Exception):
            return ""

    # ------------------------------------------------------------------
    # Wallet creation & listing
    # ------------------------------------------------------------------

    async def create_wallets(self, count: int) -> list[str]:
        """Generate *count* new wallets via private key, persist to DB and backup file."""
        os.makedirs("data", exist_ok=True)
        addresses: list[str] = []

        async with aiosqlite.connect(DB_PATH) as db:
            with open("data/genedWallet.txt", "a", encoding="utf-8") as f:
                for _ in range(count):
                    acct = Account.create()
                    address = acct.address
                    private_key = acct.key.hex()

                    await db.execute(
                        """
                        INSERT INTO wallets (address, private_key, mnemonic, label, chain)
                        VALUES (?, ?, NULL, ?, ?)
                        """,
                        (address, private_key, f"Wallet_{address[-4:]}", DEFAULT_CHAIN),
                    )
                    f.write(f"{address} | {private_key}\n")
                    addresses.append(address)
            await db.commit()

        return addresses

    async def list_wallets(self) -> list[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT id, address, label, chain, created_at FROM wallets ORDER BY id DESC"
            )
            rows = await cur.fetchall()
            return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # BNB balances
    # ------------------------------------------------------------------

    async def get_wallet_balance(self, address: str) -> str:
        checksum = self.validate_evm_address(address)
        if not checksum:
            raise ValueError("invalid address")
        w3 = await self._rpc.get_http()
        balance_wei = await w3.eth.get_balance(checksum)
        return str(Decimal(w3.from_wei(balance_wei, "ether")))

    async def get_wallet_balances(self, addresses: list[str]) -> dict[str, str]:
        async def _fetch(address: str) -> tuple[str, str]:
            try:
                return address, await self.get_wallet_balance(address)
            except Exception:
                return address, "0"

        pairs = await asyncio.gather(*(_fetch(a) for a in addresses))
        return dict(pairs)

    # ------------------------------------------------------------------
    # Private key access
    # ------------------------------------------------------------------

    async def get_private_key(self, wallet_id: int) -> str:
        """Return the raw private key hex for wallet *wallet_id*."""
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT private_key FROM wallets WHERE id = ?", (wallet_id,)
            )
            row = await cur.fetchone()
        if not row:
            raise ValueError(f"wallet #{wallet_id} not found")
        return row["private_key"]

    # ------------------------------------------------------------------
    # Token holdings
    # ------------------------------------------------------------------

    async def get_token_holdings(self, wallet_id: int) -> list[dict]:
        """
        Return all ERC-20 tokens with a non-zero balance held by wallet *wallet_id*.

        Discovery strategy:
          1. Find all unique token addresses from copy-trade transactions
             linked to this wallet.
          2. Check each token's on-chain balance.
          3. Estimate BNB value via PancakeSwap getAmountsOut.
          4. Return non-zero holdings sorted by BNB value descending.
        """
        wallet = await self._load_wallet_by_id(wallet_id)
        if not wallet:
            return []

        wallet_address = AsyncWeb3.to_checksum_address(wallet["address"])

        # Discover token addresses from our transaction history
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT DISTINCT t.token
                FROM transactions t
                JOIN copy_tasks ct ON t.source_task_id = ct.id AND t.source_task_type = 'copy'
                WHERE ct.wallet_id = ?
                  AND t.action = 'buy'
                  AND t.token NOT IN ('', 'UNKNOWN')
                """,
                (wallet_id,),
            )
            rows = await cur.fetchall()
        token_addresses = [r["token"] for r in rows]

        if not token_addresses:
            return []

        w3 = await self._rpc.get_http()
        router = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(PANCAKESWAP_ROUTER),
            abi=_ROUTER_ABI,
        )
        wbnb = AsyncWeb3.to_checksum_address(WBNB_ADDRESS)
        holdings: list[dict] = []

        for raw_addr in token_addresses:
            checksum = self.validate_evm_address(raw_addr)
            if not checksum:
                continue
            try:
                token_contract = w3.eth.contract(address=checksum, abi=_ERC20_ABI)
                balance_raw: int = await token_contract.functions.balanceOf(wallet_address).call()
                if balance_raw == 0:
                    continue

                decimals: int = await token_contract.functions.decimals().call()
                balance = balance_raw / 10**decimals

                # Symbol / name — some tokens may revert these
                try:
                    symbol: str = await token_contract.functions.symbol().call()
                except Exception:
                    symbol = checksum[:8] + "…"
                try:
                    token_name: str = await token_contract.functions.name().call()
                except Exception:
                    token_name = symbol

                # BNB value via router
                bnb_value = await self._get_bnb_value(router, checksum, wbnb, balance_raw)

                holdings.append(
                    {
                        "token": checksum,
                        "symbol": symbol,
                        "name": token_name,
                        "balance": balance,
                        "balance_raw": str(balance_raw),
                        "decimals": decimals,
                        "bnb_value": bnb_value,
                    }
                )
            except Exception:
                continue

        holdings.sort(key=lambda x: x["bnb_value"], reverse=True)
        return holdings

    async def _get_bnb_value(self, router, token_addr: str, wbnb: str, balance_raw: int) -> float:
        """Estimate BNB value of *balance_raw* units of *token_addr*."""
        if balance_raw == 0:
            return 0.0
        try:
            amounts = await router.functions.getAmountsOut(
                balance_raw, [token_addr, wbnb]
            ).call()
            return amounts[-1] / 1e18
        except Exception:
            return 0.0

    # ------------------------------------------------------------------
    # Transfer (BNB or ERC-20)
    # ------------------------------------------------------------------

    async def transfer(
        self,
        wallet_id: int,
        to_address: str,
        token: str,
        amount: float,
    ) -> dict:
        """
        Transfer BNB or an ERC-20 token from wallet *wallet_id*.

        Args:
            token: "" or "BNB" for native BNB transfer; otherwise the ERC-20 contract address.
            amount: human-readable amount (e.g. 0.5 BNB or 1000 TOKEN).
        """
        wallet = await self._load_wallet_by_id(wallet_id)
        if not wallet:
            return {"status": "error", "message": "wallet not found"}

        checksum_to = self.validate_evm_address(to_address.strip())
        if not checksum_to:
            return {"status": "error", "message": "invalid recipient address"}

        try:
            w3 = await self._rpc.get_http()
            account = Account.from_key(wallet["private_key"])
            nonce = await w3.eth.get_transaction_count(account.address)
            gas_price = await w3.eth.gas_price

            if not token or token.upper() == "BNB":
                # ── Native BNB transfer ──
                gas_limit = 21_000
                gas_cost = gas_limit * gas_price
                amount_wei = w3.to_wei(amount, "ether")

                # Guard: amount + gas must not exceed on-chain balance
                balance_wei = await w3.eth.get_balance(account.address)
                if amount_wei + gas_cost > balance_wei:
                    amount_wei = balance_wei - gas_cost
                    if amount_wei <= 0:
                        return {"status": "error", "message": "余额不足以支付 gas 费用"}

                raw_tx = {
                    "to": checksum_to,
                    "value": amount_wei,
                    "gas": gas_limit,
                    "gasPrice": gas_price,
                    "nonce": nonce,
                    "chainId": 56,
                }
                signed = account.sign_transaction(raw_tx)
                tx_hash = await w3.eth.send_raw_transaction(signed.rawTransaction)
                return {"status": "submitted", "tx_hash": tx_hash.hex(), "token": "BNB"}

            else:
                # ── ERC-20 transfer ──
                checksum_token = self.validate_evm_address(token)
                if not checksum_token:
                    return {"status": "error", "message": "invalid token address"}

                token_contract = w3.eth.contract(address=checksum_token, abi=_ERC20_ABI)
                decimals: int = await token_contract.functions.decimals().call()
                amount_raw = int(amount * 10**decimals)

                # Estimate gas with 20 % buffer; fall back to a safe limit
                try:
                    estimated = await token_contract.functions.transfer(
                        checksum_to, amount_raw
                    ).estimate_gas({"from": account.address})
                    gas_limit = int(estimated * 1.2)
                except Exception:
                    gas_limit = 100_000

                tx = await token_contract.functions.transfer(
                    checksum_to, amount_raw
                ).build_transaction(
                    {
                        "from": account.address,
                        "nonce": nonce,
                        "gasPrice": gas_price,
                        "gas": gas_limit,
                        "chainId": 56,
                    }
                )
                signed = account.sign_transaction(tx)
                tx_hash = await w3.eth.send_raw_transaction(signed.rawTransaction)
                return {"status": "submitted", "tx_hash": tx_hash.hex(), "token": checksum_token}

        except Exception as exc:
            return {"status": "error", "message": str(exc)}

    # ------------------------------------------------------------------
    # Panic sell (full position via PancakeSwap)
    # ------------------------------------------------------------------

    async def panic_sell(
        self, wallet_address: str, token_address: str, slippage: int = 3
    ) -> dict:
        """Sell ALL *token_address* holdings for BNB via PancakeSwap V2."""
        checksum_wallet = self.validate_evm_address(wallet_address)
        checksum_token = self.validate_evm_address(token_address)
        if not checksum_wallet or not checksum_token:
            return {"status": "error", "message": "invalid address"}

        try:
            w3 = await self._rpc.get_http()
            private_key = await self._get_private_key_by_address(checksum_wallet)
            account = Account.from_key(private_key)

            token_contract = w3.eth.contract(address=checksum_token, abi=_ERC20_ABI)
            balance: int = await token_contract.functions.balanceOf(checksum_wallet).call()
            decimals: int = await token_contract.functions.decimals().call()

            if balance == 0:
                return {"status": "error", "message": "zero token balance"}

            router = w3.eth.contract(
                address=AsyncWeb3.to_checksum_address(PANCAKESWAP_ROUTER), abi=_ROUTER_ABI
            )
            wbnb = AsyncWeb3.to_checksum_address(WBNB_ADDRESS)
            path = [checksum_token, wbnb]
            amounts_out = await router.functions.getAmountsOut(balance, path).call()
            amount_out_min = int(amounts_out[-1] * (100 - slippage) / 100)
            deadline = int(time.time()) + 300

            nonce = await w3.eth.get_transaction_count(checksum_wallet)
            gas_price = await w3.eth.gas_price

            approve_tx = await token_contract.functions.approve(
                AsyncWeb3.to_checksum_address(PANCAKESWAP_ROUTER), balance
            ).build_transaction(
                {"from": checksum_wallet, "nonce": nonce, "gasPrice": gas_price}
            )
            signed_approve = account.sign_transaction(approve_tx)
            await w3.eth.send_raw_transaction(signed_approve.rawTransaction)

            swap_tx = await router.functions.swapExactTokensForETH(
                balance, amount_out_min, path, checksum_wallet, deadline
            ).build_transaction(
                {"from": checksum_wallet, "nonce": nonce + 1, "gasPrice": gas_price}
            )
            signed_swap = account.sign_transaction(swap_tx)
            tx_hash = await w3.eth.send_raw_transaction(signed_swap.rawTransaction)

            return {
                "status": "submitted",
                "tx_hash": tx_hash.hex(),
                "token": token_address,
                "amount": str(balance / 10**decimals),
            }

        except Exception as exc:
            return {"status": "error", "message": str(exc)}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    # ------------------------------------------------------------------
    # Rename
    # ------------------------------------------------------------------

    async def update_name(self, wallet_id: int, name: str) -> bool:
        """Update the display name (label) of wallet *wallet_id*. Returns False if not found."""
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "UPDATE wallets SET label = ? WHERE id = ?", (name.strip(), wallet_id)
            )
            await db.commit()
            return cur.rowcount > 0

    async def _load_wallet_by_id(self, wallet_id: int) -> dict | None:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT id, address, private_key FROM wallets WHERE id = ?", (wallet_id,)
            )
            row = await cur.fetchone()
            return dict(row) if row else None

    async def _get_private_key_by_address(self, address: str) -> str:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT private_key FROM wallets WHERE address = ?", (address,)
            )
            row = await cur.fetchone()
        if not row:
            raise ValueError(f"wallet {address} not found")
        return row["private_key"]
