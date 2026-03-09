"""Trade execution router — Flap, Fourmeme, PancakeSwap V2.

Handles buy, sell, token approval, and price estimation across all
supported platforms.
"""

from __future__ import annotations

import logging
import time

from eth_account import Account
from web3 import AsyncWeb3

from core.constants import (
    PANCAKESWAP_ROUTER,
    PLATFORM_CONTRACTS,
    WBNB_ADDRESS,
)
from rpc.manager import RpcManager

logger = logging.getLogger(__name__)

# ── Flap Portal ABI (subset) ────────────────────────────────────────────

_FLAP_ABI = [
    {
        "name": "buy",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [
            {"name": "", "type": "address"},
            {"name": "", "type": "address"},
            {"name": "", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "sell",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "", "type": "address"},
            {"name": "", "type": "uint256"},
            {"name": "", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "previewBuy",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "", "type": "address"},
            {"name": "", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "previewSell",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "", "type": "address"},
            {"name": "", "type": "uint256"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]

# ── Four.meme TokenManager V2 ABI (subset) ──────────────────────────────

_FOURMEME_ABI = [
    # buyTokenAMAP(token, funds, minAmount) — "As Much As Possible"
    {
        "name": "buyTokenAMAP",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [
            {"name": "token", "type": "address"},
            {"name": "funds", "type": "uint256"},
            {"name": "minAmount", "type": "uint256"},
        ],
        "outputs": [],
    },
    # sellToken(token, amount, minFunds)
    {
        "name": "sellToken",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "token", "type": "address"},
            {"name": "amount", "type": "uint256"},
            {"name": "minFunds", "type": "uint256"},
        ],
        "outputs": [],
    },
    # _tokenInfos(token) — returns struct with lastPrice, K, T, etc.
    {
        "name": "_tokenInfos",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "", "type": "address"}],
        "outputs": [
            {"name": "base", "type": "address"},
            {"name": "quote", "type": "address"},
            {"name": "template", "type": "uint256"},
            {"name": "totalSupply", "type": "uint256"},
            {"name": "maxOffers", "type": "uint256"},
            {"name": "maxRaising", "type": "uint256"},
            {"name": "launchTime", "type": "uint256"},
            {"name": "offers", "type": "uint256"},
            {"name": "funds", "type": "uint256"},
            {"name": "lastPrice", "type": "uint256"},
            {"name": "K", "type": "uint256"},
            {"name": "T", "type": "uint256"},
            {"name": "status", "type": "uint256"},
        ],
    },
]

# ── PancakeSwap V2 Router ABI (subset) ──────────────────────────────────

_PCS_ROUTER_ABI = [
    {
        "name": "swapExactETHForTokens",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
    },
    {
        "name": "swapExactTokensForETH",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "amountOutMin", "type": "uint256"},
            {"name": "path", "type": "address[]"},
            {"name": "to", "type": "address"},
            {"name": "deadline", "type": "uint256"},
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
    },
    {
        "name": "getAmountsOut",
        "type": "function",
        "stateMutability": "view",
        "inputs": [
            {"name": "amountIn", "type": "uint256"},
            {"name": "path", "type": "address[]"},
        ],
        "outputs": [{"name": "amounts", "type": "uint256[]"}],
    },
]

_ERC20_ABI = [
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
        "name": "balanceOf",
        "type": "function",
        "inputs": [{"name": "account", "type": "address"}],
        "outputs": [{"name": "", "type": "uint256"}],
    },
    {
        "name": "allowance",
        "type": "function",
        "inputs": [
            {"name": "owner", "type": "address"},
            {"name": "spender", "type": "address"},
        ],
        "outputs": [{"name": "", "type": "uint256"}],
    },
]

MAX_UINT256 = 2**256 - 1
DEFAULT_GAS_LIMIT = 500_000
GAS_ESTIMATE_BUFFER = 1.4  # 40% buffer over estimate


class TradeRouter:
    """Execute trades across Flap, Fourmeme, and PancakeSwap V2."""

    def __init__(self, rpc: RpcManager):
        self.rpc = rpc

    # ── Helpers ───────────────────────────────────────────────────────────

    async def _estimate_gas(self, w3, tx_params: dict) -> int:
        """Estimate gas with buffer, fallback to DEFAULT_GAS_LIMIT."""
        try:
            estimate = await w3.eth.estimate_gas(tx_params)
            return int(estimate * GAS_ESTIMATE_BUFFER)
        except Exception as exc:
            logger.debug("gas estimate failed (%s), using default %d", exc, DEFAULT_GAS_LIMIT)
            return DEFAULT_GAS_LIMIT

    async def _send_and_verify(self, w3, signed_tx, timeout: int = 60) -> str:
        """Send tx and wait for receipt. Raises if reverted. Returns tx_hash hex."""
        tx_hash = await w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        receipt = await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
        if receipt.get("status") == 0:
            raise RuntimeError(f"tx reverted: {tx_hash.hex()}")
        return tx_hash.hex()

    # ── Buy ──────────────────────────────────────────────────────────────

    async def buy(
        self,
        platform: str,
        token: str,
        amount_bnb_wei: int,
        private_key: str,
        slippage: int = 5,
        gas_multiplier: float = 1.2,
        chain: str = "bsc",
    ) -> dict:
        """Buy tokens.  Returns ``{tx_hash, estimated_tokens}``."""
        w3 = await self.rpc.get_http(chain)
        account = Account.from_key(private_key)
        gas_price = int(await w3.eth.gas_price * gas_multiplier)
        nonce = await w3.eth.get_transaction_count(account.address)
        token_cs = AsyncWeb3.to_checksum_address(token)

        if platform == "flap":
            return await self._flap_buy(
                w3, account, token_cs, amount_bnb_wei, slippage, gas_price, nonce,
            )
        elif platform == "fourmeme":
            return await self._fourmeme_buy(
                w3, account, token_cs, amount_bnb_wei, slippage, gas_price, nonce,
            )
        elif platform == "dex":
            return await self._pcs_buy(
                w3, account, token_cs, amount_bnb_wei, slippage, gas_price, nonce,
            )
        else:
            raise ValueError(f"unsupported platform: {platform}")

    async def _flap_buy(self, w3, account, token, amount_wei, slippage, gas_price, nonce):
        contract_addr = AsyncWeb3.to_checksum_address(PLATFORM_CONTRACTS["flap"])
        contract = w3.eth.contract(address=contract_addr, abi=_FLAP_ABI)

        estimated = await contract.functions.previewBuy(token, amount_wei).call()
        min_out = int(estimated * (100 - slippage) / 100)

        tx_params = {
            "from": account.address,
            "value": amount_wei,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": 56,
        }
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": contract_addr,
            "data": contract.functions.buy(token, account.address, min_out)._encode_transaction_data(),
        })
        tx = await contract.functions.buy(
            token, account.address, min_out,
        ).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        return {"tx_hash": tx_hash, "estimated_tokens": str(estimated)}

    async def _fourmeme_buy(self, w3, account, token, amount_wei, slippage, gas_price, nonce):
        contract_addr = AsyncWeb3.to_checksum_address(PLATFORM_CONTRACTS["fourmeme"])
        contract = w3.eth.contract(address=contract_addr, abi=_FOURMEME_ABI)

        # Estimate tokens from lastPrice for slippage protection
        estimated = 0
        min_amount = 0
        try:
            info = await contract.functions._tokenInfos(token).call()
            last_price = info[9]  # lastPrice field
            if last_price > 0:
                estimated = amount_wei * 10**18 // last_price
                min_amount = int(estimated * (100 - slippage) / 100)
        except Exception:
            pass

        tx_params = {
            "from": account.address,
            "value": amount_wei,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": 56,
        }
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": contract_addr,
            "data": contract.functions.buyTokenAMAP(token, amount_wei, min_amount)._encode_transaction_data(),
        })
        tx = await contract.functions.buyTokenAMAP(
            token, amount_wei, min_amount,
        ).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        return {"tx_hash": tx_hash, "estimated_tokens": str(estimated)}

    async def _pcs_buy(self, w3, account, token, amount_wei, slippage, gas_price, nonce):
        router_addr = AsyncWeb3.to_checksum_address(PANCAKESWAP_ROUTER)
        router = w3.eth.contract(address=router_addr, abi=_PCS_ROUTER_ABI)
        wbnb = AsyncWeb3.to_checksum_address(WBNB_ADDRESS)

        amounts = await router.functions.getAmountsOut(amount_wei, [wbnb, token]).call()
        min_out = int(amounts[-1] * (100 - slippage) / 100)
        deadline = int(time.time()) + 300

        tx_params = {
            "from": account.address,
            "value": amount_wei,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": 56,
        }
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": router_addr,
            "data": router.functions.swapExactETHForTokens(
                min_out, [wbnb, token], account.address, deadline,
            )._encode_transaction_data(),
        })
        tx = await router.functions.swapExactETHForTokens(
            min_out, [wbnb, token], account.address, deadline,
        ).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        return {"tx_hash": tx_hash, "estimated_tokens": str(amounts[-1])}

    # ── Sell ─────────────────────────────────────────────────────────────

    async def sell(
        self,
        platform: str,
        token: str,
        amount_token_raw: int,
        private_key: str,
        slippage: int = 5,
        gas_multiplier: float = 1.2,
        chain: str = "bsc",
    ) -> dict:
        """Sell tokens for BNB.  Returns ``{tx_hash, estimated_bnb}``."""
        w3 = await self.rpc.get_http(chain)
        account = Account.from_key(private_key)
        gas_price = int(await w3.eth.gas_price * gas_multiplier)
        nonce = await w3.eth.get_transaction_count(account.address)
        token_cs = AsyncWeb3.to_checksum_address(token)

        if platform == "flap":
            return await self._flap_sell(
                w3, account, token_cs, amount_token_raw, slippage, gas_price, nonce,
            )
        elif platform == "fourmeme":
            return await self._fourmeme_sell(
                w3, account, token_cs, amount_token_raw, slippage, gas_price, nonce,
            )
        elif platform == "dex":
            return await self._pcs_sell(
                w3, account, token_cs, amount_token_raw, slippage, gas_price, nonce,
            )
        else:
            raise ValueError(f"unsupported platform: {platform}")

    async def _ensure_approval(self, w3, account, token, spender, amount, gas_price, nonce):
        """Approve spender if current allowance is insufficient.  Returns updated nonce."""
        erc20 = w3.eth.contract(address=token, abi=_ERC20_ABI)
        allowance = await erc20.functions.allowance(account.address, spender).call()
        if allowance >= amount:
            return nonce
        tx = await erc20.functions.approve(spender, MAX_UINT256).build_transaction({
            "from": account.address,
            "gas": 100_000,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": 56,
        })
        signed = account.sign_transaction(tx)
        tx_hash = await w3.eth.send_raw_transaction(signed.rawTransaction)
        # Wait for approval to be mined before proceeding with sell
        await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
        return nonce + 1

    async def _flap_sell(self, w3, account, token, amount, slippage, gas_price, nonce):
        contract_addr = AsyncWeb3.to_checksum_address(PLATFORM_CONTRACTS["flap"])
        contract = w3.eth.contract(address=contract_addr, abi=_FLAP_ABI)

        nonce = await self._ensure_approval(
            w3, account, token, contract_addr, amount, gas_price, nonce,
        )

        estimated_bnb = await contract.functions.previewSell(token, amount).call()
        min_out = int(estimated_bnb * (100 - slippage) / 100)

        tx_params = {
            "from": account.address,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": 56,
        }
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": contract_addr,
            "data": contract.functions.sell(token, amount, min_out)._encode_transaction_data(),
        })
        tx = await contract.functions.sell(token, amount, min_out).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        return {"tx_hash": tx_hash, "estimated_bnb": estimated_bnb / 1e18}

    async def _fourmeme_sell(self, w3, account, token, amount, slippage, gas_price, nonce):
        contract_addr = AsyncWeb3.to_checksum_address(PLATFORM_CONTRACTS["fourmeme"])

        nonce = await self._ensure_approval(
            w3, account, token, contract_addr, amount, gas_price, nonce,
        )

        contract = w3.eth.contract(address=contract_addr, abi=_FOURMEME_ABI)

        # Estimate BNB output from lastPrice for slippage protection
        estimated_bnb_wei = 0
        min_funds = 0
        try:
            info = await contract.functions._tokenInfos(token).call()
            last_price = info[9]  # lastPrice field
            if last_price > 0:
                estimated_bnb_wei = amount * last_price // 10**18
                min_funds = int(estimated_bnb_wei * (100 - slippage) / 100)
        except Exception:
            pass

        tx_params = {
            "from": account.address,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": 56,
        }
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": contract_addr,
            "data": contract.functions.sellToken(token, amount, min_funds)._encode_transaction_data(),
        })
        tx = await contract.functions.sellToken(
            token, amount, min_funds,
        ).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        return {"tx_hash": tx_hash, "estimated_bnb": estimated_bnb_wei / 1e18}

    async def _pcs_sell(self, w3, account, token, amount, slippage, gas_price, nonce):
        router_addr = AsyncWeb3.to_checksum_address(PANCAKESWAP_ROUTER)
        wbnb = AsyncWeb3.to_checksum_address(WBNB_ADDRESS)

        nonce = await self._ensure_approval(
            w3, account, token, router_addr, amount, gas_price, nonce,
        )

        router = w3.eth.contract(address=router_addr, abi=_PCS_ROUTER_ABI)
        amounts = await router.functions.getAmountsOut(amount, [token, wbnb]).call()
        min_out = int(amounts[-1] * (100 - slippage) / 100)
        deadline = int(time.time()) + 300

        tx_params = {
            "from": account.address,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": 56,
        }
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": router_addr,
            "data": router.functions.swapExactTokensForETH(
                amount, min_out, [token, wbnb], account.address, deadline,
            )._encode_transaction_data(),
        })
        tx = await router.functions.swapExactTokensForETH(
            amount, min_out, [token, wbnb], account.address, deadline,
        ).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        return {"tx_hash": tx_hash, "estimated_bnb": amounts[-1] / 1e18}

    # ── Price estimation ─────────────────────────────────────────────────

    async def get_sell_value_bnb(
        self, platform: str, token: str, amount_token_raw: int, chain: str = "bsc",
    ) -> float:
        """Estimate BNB received for selling *amount_token_raw* tokens."""
        if amount_token_raw == 0:
            return 0.0
        try:
            w3 = await self.rpc.get_http(chain)
            token_cs = AsyncWeb3.to_checksum_address(token)

            if platform == "flap":
                contract = w3.eth.contract(
                    address=AsyncWeb3.to_checksum_address(PLATFORM_CONTRACTS["flap"]),
                    abi=_FLAP_ABI,
                )
                bnb_wei = await contract.functions.previewSell(token_cs, amount_token_raw).call()
                return bnb_wei / 1e18

            elif platform == "dex":
                router = w3.eth.contract(
                    address=AsyncWeb3.to_checksum_address(PANCAKESWAP_ROUTER),
                    abi=_PCS_ROUTER_ABI,
                )
                wbnb = AsyncWeb3.to_checksum_address(WBNB_ADDRESS)
                amounts = await router.functions.getAmountsOut(
                    amount_token_raw, [token_cs, wbnb],
                ).call()
                return amounts[-1] / 1e18

            elif platform == "fourmeme":
                contract = w3.eth.contract(
                    address=AsyncWeb3.to_checksum_address(PLATFORM_CONTRACTS["fourmeme"]),
                    abi=_FOURMEME_ABI,
                )
                info = await contract.functions._tokenInfos(token_cs).call()
                last_price = info[9]  # lastPrice field
                if last_price > 0:
                    return (amount_token_raw * last_price) / 1e36
                return 0.0

            else:
                return 0.0
        except Exception:
            return 0.0

    async def get_token_balance(
        self, token: str, wallet_address: str, chain: str = "bsc",
    ) -> int:
        """Get on-chain token balance for *wallet_address*."""
        w3 = await self.rpc.get_http(chain)
        erc20 = w3.eth.contract(
            address=AsyncWeb3.to_checksum_address(token),
            abi=_ERC20_ABI,
        )
        return await erc20.functions.balanceOf(
            AsyncWeb3.to_checksum_address(wallet_address),
        ).call()
