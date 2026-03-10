"""Trade execution router — Flap, Fourmeme, PancakeSwap V2 & V3.

Handles buy, sell, token approval, and price estimation across all
supported platforms, with auto-detection for PCS graduation.
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

# ── PCS Constants ───────────────────────────────────────────────────────
PANCAKESWAP_V2_FACTORY = "0xcA143Ce32Fe78f1f7019d7d551a6402fC5350c73"
PANCAKESWAP_V3_FACTORY = "0x0BFbCF9fa4f9C56B0F40a671Ad40E0805A091865"
PCS_V3_QUOTER = "0xB048Bbc1Ee6b733FFfCFb9e9CeF7375518e25997"
PANCAKESWAP_V3_ROUTER = "0x13f4EA83D0bd40E75C8222255bc855a974568Dd4"

# ── Factory ABIs ────────────────────────────────────────────────────────
_PCS_V2_FACTORY_ABI = [
    {"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"}],
     "name":"getPair","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}
]
_PCS_V3_FACTORY_ABI = [
    {"inputs":[{"internalType":"address","name":"","type":"address"},{"internalType":"address","name":"","type":"address"},{"internalType":"uint24","name":"","type":"uint24"}],
     "name":"getPool","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}
]

# ── Flap Portal ABI (subset) ────────────────────────────────────────────
_FLAP_ABI = [
    {"name": "buy", "type": "function", "stateMutability": "payable", "inputs": [{"name": "", "type": "address"}, {"name": "", "type": "address"}, {"name": "", "type": "uint256"}], "outputs": [{"name": "", "type": "uint256"}]},
    {"name": "sell", "type": "function", "stateMutability": "nonpayable", "inputs": [{"name": "", "type": "address"}, {"name": "", "type": "uint256"}, {"name": "", "type": "uint256"}], "outputs": [{"name": "", "type": "uint256"}]},
    {"name": "previewBuy", "type": "function", "stateMutability": "view", "inputs": [{"name": "", "type": "address"}, {"name": "", "type": "uint256"}], "outputs": [{"name": "", "type": "uint256"}]},
    {"name": "previewSell", "type": "function", "stateMutability": "view", "inputs": [{"name": "", "type": "address"}, {"name": "", "type": "uint256"}], "outputs": [{"name": "", "type": "uint256"}]},
    {
        "name": "getTokenV7",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "", "type": "address"}],
        "outputs": [{
            "name": "",
            "type": "tuple",
            "components": [
                {"name": "status", "type": "uint8"},
                {"name": "reserve", "type": "uint256"},
                {"name": "circulatingSupply", "type": "uint256"},
                {"name": "price", "type": "uint256"},
                {"name": "tokenVersion", "type": "uint8"},
                {"name": "r", "type": "uint256"},
                {"name": "h", "type": "uint256"},
                {"name": "k", "type": "uint256"},
                {"name": "dexSupplyThresh", "type": "uint256"},
                {"name": "quoteTokenAddress", "type": "address"},
                {"name": "nativeToQuoteSwapEnabled", "type": "bool"},
                {"name": "extensionID", "type": "bytes32"},
                {"name": "taxRate", "type": "uint256"},
                {"name": "pool", "type": "address"},
                {"name": "progress", "type": "uint256"},
                {"name": "lpFeeProfile", "type": "uint8"},
                {"name": "dexId", "type": "uint8"}
            ]
        }]
    },
    {
        "name": "getTokenV6",
        "type": "function",
        "stateMutability": "view",
        "inputs": [{"name": "", "type": "address"}],
        "outputs": [{
            "name": "",
            "type": "tuple",
            "components": [
                {"name": "status", "type": "uint8"},
                {"name": "reserve", "type": "uint256"},
                {"name": "circulatingSupply", "type": "uint256"},
                {"name": "price", "type": "uint256"},
                {"name": "tokenVersion", "type": "uint8"},
                {"name": "r", "type": "uint256"},
                {"name": "h", "type": "uint256"},
                {"name": "k", "type": "uint256"},
                {"name": "dexSupplyThresh", "type": "uint256"},
                {"name": "quoteTokenAddress", "type": "address"},
                {"name": "nativeToQuoteSwapEnabled", "type": "bool"},
                {"name": "extensionID", "type": "bytes32"},
                {"name": "taxRate", "type": "uint256"},
                {"name": "pool", "type": "address"},
                {"name": "progress", "type": "uint256"}
            ]
        }]
    },
]

# ── Four.meme TokenManager V2 ABI (subset) ──────────────────────────────
_FOURMEME_ABI = [
    {"name": "buyTokenAMAP", "type": "function", "stateMutability": "payable", "inputs": [{"name": "token", "type": "address"}, {"name": "funds", "type": "uint256"}, {"name": "minAmount", "type": "uint256"}], "outputs": []},
    {"name": "sellToken", "type": "function", "stateMutability": "nonpayable", "inputs": [{"name": "token", "type": "address"}, {"name": "amount", "type": "uint256"}, {"name": "minFunds", "type": "uint256"}], "outputs": []},
    {"name": "_tokenInfos", "type": "function", "stateMutability": "view", "inputs": [{"name": "", "type": "address"}], "outputs": [{"name": "base", "type": "address"}, {"name": "quote", "type": "address"}, {"name": "template", "type": "uint256"}, {"name": "totalSupply", "type": "uint256"}, {"name": "maxOffers", "type": "uint256"}, {"name": "maxRaising", "type": "uint256"}, {"name": "launchTime", "type": "uint256"}, {"name": "offers", "type": "uint256"}, {"name": "funds", "type": "uint256"}, {"name": "lastPrice", "type": "uint256"}, {"name": "K", "type": "uint256"}, {"name": "T", "type": "uint256"}, {"name": "status", "type": "uint256"}]},
]

_FOURMEME_TAX_TOKEN_ABI = [
    {
        "inputs": [],
        "name": "feeRate",
        "outputs": [{"internalType": "uint256", "name": "", "type": "uint256"}],
        "stateMutability": "view",
        "type": "function"
    }
]
# ── PancakeSwap V2 Router ABI (subset) ──────────────────────────────────
_PCS_ROUTER_ABI = [
    {"name": "swapExactETHForTokens", "type": "function", "stateMutability": "payable", "inputs": [{"name": "amountOutMin", "type": "uint256"}, {"name": "path", "type": "address[]"}, {"name": "to", "type": "address"}, {"name": "deadline", "type": "uint256"}], "outputs": [{"name": "amounts", "type": "uint256[]"}]},
    {"name": "swapExactTokensForETH", "type": "function", "stateMutability": "nonpayable", "inputs": [{"name": "amountIn", "type": "uint256"}, {"name": "amountOutMin", "type": "uint256"}, {"name": "path", "type": "address[]"}, {"name": "to", "type": "address"}, {"name": "deadline", "type": "uint256"}], "outputs": [{"name": "amounts", "type": "uint256[]"}]},
    {"name": "getAmountsOut", "type": "function", "stateMutability": "view", "inputs": [{"name": "amountIn", "type": "uint256"}, {"name": "path", "type": "address[]"}], "outputs": [{"name": "amounts", "type": "uint256[]"}]},
]

# ── PancakeSwap V3 SmartRouter ABI (subset) ───────────────────────────
_PCS_V3_ROUTER_ABI = [
    {
        "name": "exactInputSingle",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [{
            "name": "params",
            "type": "tuple",
            "components": [
                {"name": "tokenIn", "type": "address"},
                {"name": "tokenOut", "type": "address"},
                {"name": "fee", "type": "uint24"},
                {"name": "recipient", "type": "address"},
                {"name": "amountIn", "type": "uint256"},
                {"name": "amountOutMinimum", "type": "uint256"},
                {"name": "sqrtPriceLimitX96", "type": "uint160"},
            ],
        }],
        "outputs": [{"name": "amountOut", "type": "uint256"}],
    },
    {
        "name": "multicall",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [{"name": "data", "type": "bytes[]"}],
        "outputs": [{"name": "results", "type": "bytes[]"}],
    },
    {
        "name": "unwrapWETH9",
        "type": "function",
        "stateMutability": "payable",
        "inputs": [
            {"name": "amountMinimum", "type": "uint256"},
            {"name": "recipient", "type": "address"},
        ],
        "outputs": [],
    },
]

# ── PancakeSwap V3 QuoterV2 ABI (subset) ─────────────────────────────
_PCS_V3_QUOTER_ABI = [
    {
        "name": "quoteExactInputSingle",
        "type": "function",
        "stateMutability": "nonpayable",
        "inputs": [{
            "name": "params",
            "type": "tuple",
            "components": [
                {"name": "tokenIn", "type": "address"},
                {"name": "tokenOut", "type": "address"},
                {"name": "amountIn", "type": "uint256"},
                {"name": "fee", "type": "uint24"},
                {"name": "sqrtPriceLimitX96", "type": "uint160"},
            ],
        }],
        "outputs": [
            {"name": "amountOut", "type": "uint256"},
            {"name": "sqrtPriceX96After", "type": "uint160"},
            {"name": "initializedTicksCrossed", "type": "uint32"},
            {"name": "gasEstimate", "type": "uint256"},
        ],
    },
]

_ERC20_ABI = [
    {"name": "approve", "type": "function", "inputs": [{"name": "spender", "type": "address"}, {"name": "amount", "type": "uint256"}], "outputs": [{"name": "", "type": "bool"}]},
    {"name": "balanceOf", "type": "function", "inputs": [{"name": "account", "type": "address"}], "outputs": [{"name": "", "type": "uint256"}]},
    {"name": "allowance", "type": "function", "inputs": [{"name": "owner", "type": "address"}, {"name": "spender", "type": "address"}], "outputs": [{"name": "", "type": "uint256"}]},
]

V3_FEE_TIERS = [2500, 10000, 500, 100]
_ADDRESS_THIS = "0x0000000000000000000000000000000000000002"
MAX_UINT256 = 2**256 - 1
DEFAULT_GAS_LIMIT = 500_000
GAS_ESTIMATE_BUFFER = 1.4  


class TradeRouter:
    """Execute trades across Flap, Fourmeme, and PancakeSwap V2 & V3."""

    def __init__(self, rpc: RpcManager):
        self.rpc = rpc

    # ── Helpers ───────────────────────────────────────────────────────────

    async def _estimate_gas(self, w3, tx_params: dict) -> int:
        try:
            estimate = await w3.eth.estimate_gas(tx_params)
            return int(estimate * GAS_ESTIMATE_BUFFER)
        except Exception as exc:
            logger.debug("gas estimate failed (%s), using default %d", exc, DEFAULT_GAS_LIMIT)
            return DEFAULT_GAS_LIMIT

    async def _send_and_verify(self, w3, signed_tx, timeout: int = 60) -> str:
        tx_hash = await w3.eth.send_raw_transaction(signed_tx.rawTransaction)
        receipt = await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
        if receipt.get("status") == 0:
            raise RuntimeError(f"tx reverted: {tx_hash.hex()}")
        return tx_hash.hex()

    # ── Factory Liquidity Check ───────────────────────────────────────────

    async def check_dex_liquidity(self, token: str, chain: str) -> tuple[str, int] | None:
        """Prioritize PCS V3, then V2. Returns ('v3', fee) or ('v2', 0) or None."""
        w3 = await self.rpc.get_http(chain)
        token_cs = AsyncWeb3.to_checksum_address(token)
        wbnb = AsyncWeb3.to_checksum_address(WBNB_ADDRESS)

        test_amount = int(0.0001 * 1e18)

        try:
            quoter = w3.eth.contract(address=AsyncWeb3.to_checksum_address(PCS_V3_QUOTER), abi=_PCS_V3_QUOTER_ABI)
            best_fee = None
            best_out = 0
            
            for fee in V3_FEE_TIERS:
                try:
                    quote = await quoter.functions.quoteExactInputSingle((wbnb, token_cs, test_amount, fee, 0)).call()
                    if quote[0] > best_out:
                        best_out = quote[0]
                        best_fee = fee
                except Exception:
                    continue
            
            if best_fee is not None:
                return ("v3", best_fee)
                
        except Exception as e:
            logger.debug("V3 liquidity check failed: %s", e)

        try:
            factory_v2 = w3.eth.contract(address=AsyncWeb3.to_checksum_address(PANCAKESWAP_V2_FACTORY), abi=_PCS_V2_FACTORY_ABI)
            pair = await factory_v2.functions.getPair(token_cs, wbnb).call()
            if pair != "0x0000000000000000000000000000000000000000":
                return ("v2", 0)
        except Exception as e:
            logger.debug("V2 factory check failed: %s", e)

        return None

    # ── Buy ──────────────────────────────────────────────────────────────

    async def buy(
        self,
        platform: str,
        token: str,
        amount_bnb_wei: int,
        private_key: str,
        slippage: int = 10,
        gas_multiplier: float = 1.2,
        chain: str = "bsc",
    ) -> dict:
        w3 = await self.rpc.get_http(chain)
        account = Account.from_key(private_key)
        gas_price = int(await w3.eth.gas_price * gas_multiplier)
        nonce = await w3.eth.get_transaction_count(account.address)
        token_cs = AsyncWeb3.to_checksum_address(token)

        dex_info = await self.check_dex_liquidity(token_cs, chain)
        if dex_info:
            platform = "dex"

        if platform == "flap":
            return await self._flap_buy(w3, account, token_cs, amount_bnb_wei, slippage, gas_price, nonce)
        elif platform == "fourmeme":
            return await self._fourmeme_buy(w3, account, token_cs, amount_bnb_wei, slippage, gas_price, nonce)
        elif platform == "dex":
            if dex_info and dex_info[0] == "v3":
                return await self._pcs_v3_buy(w3, account, token_cs, amount_bnb_wei, slippage, gas_price, nonce, dex_info[1])
            else:
                return await self._pcs_v2_buy(w3, account, token_cs, amount_bnb_wei, slippage, gas_price, nonce)
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
            "chainId": await w3.eth.chain_id,
        }
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": contract_addr,
            "data": contract.functions.buy(token, account.address, min_out)._encode_transaction_data(),
        })
        tx = await contract.functions.buy(token, account.address, min_out).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        return {"tx_hash": tx_hash, "estimated_tokens": str(estimated)}

    async def _fourmeme_buy(self, w3, account, token, amount_wei, slippage, gas_price, nonce):
        contract_addr = AsyncWeb3.to_checksum_address(PLATFORM_CONTRACTS["fourmeme"])
        contract = w3.eth.contract(address=contract_addr, abi=_FOURMEME_ABI)

        estimated = 0
        min_amount = 0
        try:
            info = await contract.functions._tokenInfos(token).call()
            last_price = info[9]  
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
            "chainId": await w3.eth.chain_id,
        }
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": contract_addr,
            "data": contract.functions.buyTokenAMAP(token, amount_wei, min_amount)._encode_transaction_data(),
        })
        tx = await contract.functions.buyTokenAMAP(token, amount_wei, min_amount).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        return {"tx_hash": tx_hash, "estimated_tokens": str(estimated)}

    async def _pcs_v2_buy(self, w3, account, token, amount_wei, slippage, gas_price, nonce):
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
            "chainId": await w3.eth.chain_id,
        }
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": router_addr,
            "data": router.functions.swapExactETHForTokens(min_out, [wbnb, token], account.address, deadline)._encode_transaction_data(),
        })
        tx = await router.functions.swapExactETHForTokens(min_out, [wbnb, token], account.address, deadline).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        return {"tx_hash": tx_hash, "estimated_tokens": str(amounts[-1])}

    async def _pcs_v3_buy(self, w3, account, token, amount_wei, slippage, gas_price, nonce, fee):
        router_addr = AsyncWeb3.to_checksum_address(PANCAKESWAP_V3_ROUTER)
        quoter_addr = AsyncWeb3.to_checksum_address(PCS_V3_QUOTER)
        wbnb = AsyncWeb3.to_checksum_address(WBNB_ADDRESS)

        quoter = w3.eth.contract(address=quoter_addr, abi=_PCS_V3_QUOTER_ABI)
        quote = await quoter.functions.quoteExactInputSingle((wbnb, token, amount_wei, fee, 0)).call()
        amount_out = quote[0]
        min_out = int(amount_out * (100 - slippage) / 100)

        router = w3.eth.contract(address=router_addr, abi=_PCS_V3_ROUTER_ABI)
        params = (wbnb, token, fee, account.address, amount_wei, min_out, 0)
        
        tx_params = {
            "from": account.address,
            "value": amount_wei,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": await w3.eth.chain_id,
        }
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": router_addr,
            "data": router.functions.exactInputSingle(params)._encode_transaction_data()
        })
        tx = await router.functions.exactInputSingle(params).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        return {"tx_hash": tx_hash, "estimated_tokens": str(amount_out)}

    # ── Sell ─────────────────────────────────────────────────────────────

    async def sell(
        self,
        platform: str,
        token: str,
        amount_token_raw: int,
        private_key: str,
        slippage: int = 10,
        gas_multiplier: float = 1.2,
        chain: str = "bsc",
    ) -> dict:
        w3 = await self.rpc.get_http(chain)
        account = Account.from_key(private_key)
        gas_price = int(await w3.eth.gas_price * gas_multiplier)
        nonce = await w3.eth.get_transaction_count(account.address)
        token_cs = AsyncWeb3.to_checksum_address(token)

        dex_info = await self.check_dex_liquidity(token_cs, chain)
        if dex_info:
            platform = "dex"

        if platform == "flap":
            return await self._flap_sell(w3, account, token_cs, amount_token_raw, slippage, gas_price, nonce)
        elif platform == "fourmeme":
            return await self._fourmeme_sell(w3, account, token_cs, amount_token_raw, slippage, gas_price, nonce)
        elif platform == "dex":
            if dex_info and dex_info[0] == "v3":
                return await self._pcs_v3_sell(w3, account, token_cs, amount_token_raw, slippage, gas_price, nonce, dex_info[1])
            else:
                return await self._pcs_v2_sell(w3, account, token_cs, amount_token_raw, slippage, gas_price, nonce)
        else:
            raise ValueError(f"unsupported platform: {platform}")

    async def _ensure_approval(self, w3, account, token, spender, amount, gas_price, nonce):
        erc20 = w3.eth.contract(address=token, abi=_ERC20_ABI)
        allowance = await erc20.functions.allowance(account.address, spender).call()
        if allowance >= amount:
            return nonce
        tx = await erc20.functions.approve(spender, MAX_UINT256).build_transaction({
            "from": account.address,
            "gas": 100_000,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": await w3.eth.chain_id,
        })
        signed = account.sign_transaction(tx)
        tx_hash = await w3.eth.send_raw_transaction(signed.rawTransaction)
        await w3.eth.wait_for_transaction_receipt(tx_hash, timeout=30)
        return nonce + 1

    async def _flap_sell(self, w3, account, token, amount, slippage, gas_price, nonce):
        contract_addr = AsyncWeb3.to_checksum_address(PLATFORM_CONTRACTS["flap"])
        contract = w3.eth.contract(address=contract_addr, abi=_FLAP_ABI)

        # 【优雅修复：动态查询链上税率】
        # 默认使用 100% 卖出
        safe_amount = amount
        tax_rate_bps = 0
        
        try:
            # 尝试调用 V7 接口获取代币状态
            info = await contract.functions.getTokenV7(token).call()
            tax_rate_bps = info[12]  # taxRate 在 tuple 中的索引是 12
        except Exception:
            try:
                # 兼容旧版带税代币，回退尝试 V6
                info = await contract.functions.getTokenV6(token).call()
                tax_rate_bps = info[12]
            except Exception:
                # V5 及以下的代币在内盘机制上没有税
                pass

        if tax_rate_bps > 0:
            # 如果查到确实是带税代币，计算安全卖出量
            # 为了防止链上税收乘除法产生尾部精度丢失（Dust Revert），我们在链上税率基础上再加 20 BPS (0.2%) 的安全垫
            buffer_bps = tax_rate_bps + 20
            safe_amount = amount * (10000 - buffer_bps) // 10000
            logger.info(f"Detected Tax Token on Flap (tax: {tax_rate_bps/100}%). Adjusted sell amount to {safe_amount}.")
        
        # 兜底：如果算出来的安全数量为 0，回退到原数量
        if safe_amount == 0:
            safe_amount = amount

        # 授权 safe_amount
        nonce = await self._ensure_approval(
            w3, account, token, contract_addr, safe_amount, gas_price, nonce,
        )

        # 使用算出的 safe_amount 去预览能拿到多少 BNB
        estimated_bnb = await contract.functions.previewSell(token, safe_amount).call()
        min_out = int(estimated_bnb * (100 - slippage) / 100)

        # Flap 内盘卖出竞争极度激烈，临时提权 Gas 抢跑防踩踏
        priority_gas_price = int(gas_price * 1.5)

        tx_params = {
            "from": account.address,
            "gasPrice": priority_gas_price,
            "nonce": nonce,
            "chainId": await w3.eth.chain_id,
        }
        
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": contract_addr,
            "data": contract.functions.sell(token, safe_amount, min_out)._encode_transaction_data(),
        })
        
        tx = await contract.functions.sell(token, safe_amount, min_out).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        
        return {"tx_hash": tx_hash, "estimated_bnb": estimated_bnb / 1e18}

    async def _fourmeme_sell(self, w3, account, token, amount, slippage, gas_price, nonce):
        contract_addr = AsyncWeb3.to_checksum_address(PLATFORM_CONTRACTS["fourmeme"])

        # 【优雅修复：动态查询 Four.meme 代币税率】
        safe_amount = amount
        tax_rate_bps = 0
        
        try:
            # 实例化代币本身的合约，调用 feeRate() 查税
            token_contract = w3.eth.contract(
                address=AsyncWeb3.to_checksum_address(token), 
                abi=_FOURMEME_TAX_TOKEN_ABI
            )
            tax_rate_bps = await token_contract.functions.feeRate().call()
        except Exception:
            # 如果调用失败（Revert），说明这是一个普通的 Meme 币（没有 feeRate 函数）
            # 默认 tax_rate_bps 为 0
            pass

        if tax_rate_bps > 0:
            # 如果是带税代币，计算安全卖出量
            # 假设 feeRate 返回的是 BPS (10000 = 100%)
            # 加上 20 BPS (0.2%) 的安全垫防底层精度截断导致的 Dust Revert
            buffer_bps = tax_rate_bps + 20
            safe_amount = amount * (10000 - buffer_bps) // 10000
            logger.info(f"Detected Tax Token on Four.meme (tax: {tax_rate_bps/100}%). Adjusted sell amount to {safe_amount}.")
        
        # 兜底：如果算出来的安全数量为 0，回退到原数量
        if safe_amount == 0:
            safe_amount = amount

        # 授权 safe_amount
        nonce = await self._ensure_approval(
            w3, account, token, contract_addr, safe_amount, gas_price, nonce,
        )

        contract = w3.eth.contract(address=contract_addr, abi=_FOURMEME_ABI)

        # 基于安全数量估算 BNB 输出，用于滑点保护
        estimated_bnb_wei = 0
        min_funds = 0
        try:
            info = await contract.functions._tokenInfos(token).call()
            last_price = info[9]  # lastPrice field
            if last_price > 0:
                estimated_bnb_wei = safe_amount * last_price // 10**18
                min_funds = int(estimated_bnb_wei * (100 - slippage) / 100)
        except Exception:
            pass

        # 防踩踏：Fourmeme 内盘卖出竞争激烈，提权 Gas 抢跑
        priority_gas_price = int(gas_price * 1.5)

        tx_params = {
            "from": account.address,
            "gasPrice": priority_gas_price,
            "nonce": nonce,
            "chainId": await w3.eth.chain_id,
        }
        
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": contract_addr,
            "data": contract.functions.sellToken(token, safe_amount, min_funds)._encode_transaction_data(),
        })
        
        tx = await contract.functions.sellToken(
            token, safe_amount, min_funds,
        ).build_transaction(tx_params)
        
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        
        return {"tx_hash": tx_hash, "estimated_bnb": estimated_bnb_wei / 1e18}

    async def _pcs_v2_sell(self, w3, account, token, amount, slippage, gas_price, nonce):
        router_addr = AsyncWeb3.to_checksum_address(PANCAKESWAP_ROUTER)
        wbnb = AsyncWeb3.to_checksum_address(WBNB_ADDRESS)

        nonce = await self._ensure_approval(w3, account, token, router_addr, amount, gas_price, nonce)

        router = w3.eth.contract(address=router_addr, abi=_PCS_ROUTER_ABI)
        amounts = await router.functions.getAmountsOut(amount, [token, wbnb]).call()
        min_out = int(amounts[-1] * (100 - slippage) / 100)
        deadline = int(time.time()) + 300

        tx_params = {
            "from": account.address,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": await w3.eth.chain_id,
        }
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": router_addr,
            "data": router.functions.swapExactTokensForETH(amount, min_out, [token, wbnb], account.address, deadline)._encode_transaction_data(),
        })
        tx = await router.functions.swapExactTokensForETH(amount, min_out, [token, wbnb], account.address, deadline).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        return {"tx_hash": tx_hash, "estimated_bnb": amounts[-1] / 1e18}

    async def _pcs_v3_sell(self, w3, account, token, amount, slippage, gas_price, nonce, fee):
        router_addr = AsyncWeb3.to_checksum_address(PANCAKESWAP_V3_ROUTER)
        quoter_addr = AsyncWeb3.to_checksum_address(PCS_V3_QUOTER)
        wbnb = AsyncWeb3.to_checksum_address(WBNB_ADDRESS)

        nonce = await self._ensure_approval(w3, account, token, router_addr, amount, gas_price, nonce)

        quoter = w3.eth.contract(address=quoter_addr, abi=_PCS_V3_QUOTER_ABI)
        quote = await quoter.functions.quoteExactInputSingle((token, wbnb, amount, fee, 0)).call()
        amount_out_wbnb = quote[0]
        min_out = int(amount_out_wbnb * (100 - slippage) / 100)

        router = w3.eth.contract(address=router_addr, abi=_PCS_V3_ROUTER_ABI)
        params = (token, wbnb, fee, _ADDRESS_THIS, amount, min_out, 0)
        
        data1 = router.functions.exactInputSingle(params)._encode_transaction_data()
        data2 = router.functions.unwrapWETH9(min_out, account.address)._encode_transaction_data()

        tx_params = {
            "from": account.address,
            "gasPrice": gas_price,
            "nonce": nonce,
            "chainId": await w3.eth.chain_id,
        }
        tx_params["gas"] = await self._estimate_gas(w3, {
            **tx_params,
            "to": router_addr,
            "data": router.functions.multicall([data1, data2])._encode_transaction_data()
        })
        tx = await router.functions.multicall([data1, data2]).build_transaction(tx_params)
        signed = account.sign_transaction(tx)
        tx_hash = await self._send_and_verify(w3, signed)
        return {"tx_hash": tx_hash, "estimated_bnb": amount_out_wbnb / 1e18}

    # ── Price estimation ─────────────────────────────────────────────────

    async def get_sell_value_bnb(self, platform: str, token: str, amount_token_raw: int, chain: str = "bsc") -> float:
        if amount_token_raw == 0:
            return 0.0
        try:
            w3 = await self.rpc.get_http(chain)
            token_cs = AsyncWeb3.to_checksum_address(token)

            dex_info = await self.check_dex_liquidity(token_cs, chain)
            if dex_info:
                platform = "dex"

            if platform == "flap":
                contract = w3.eth.contract(address=AsyncWeb3.to_checksum_address(PLATFORM_CONTRACTS["flap"]), abi=_FLAP_ABI)
                bnb_wei = await contract.functions.previewSell(token_cs, amount_token_raw).call()
                return bnb_wei / 1e18

            elif platform == "dex":
                if dex_info and dex_info[0] == "v3":
                    quoter = w3.eth.contract(address=AsyncWeb3.to_checksum_address(PCS_V3_QUOTER), abi=_PCS_V3_QUOTER_ABI)
                    wbnb = AsyncWeb3.to_checksum_address(WBNB_ADDRESS)
                    quote = await quoter.functions.quoteExactInputSingle((token_cs, wbnb, amount_token_raw, dex_info[1], 0)).call()
                    return quote[0] / 1e18
                else:
                    router = w3.eth.contract(address=AsyncWeb3.to_checksum_address(PANCAKESWAP_ROUTER), abi=_PCS_ROUTER_ABI)
                    wbnb = AsyncWeb3.to_checksum_address(WBNB_ADDRESS)
                    amounts = await router.functions.getAmountsOut(amount_token_raw, [token_cs, wbnb]).call()
                    return amounts[-1] / 1e18

            elif platform == "fourmeme":
                contract = w3.eth.contract(address=AsyncWeb3.to_checksum_address(PLATFORM_CONTRACTS["fourmeme"]), abi=_FOURMEME_ABI)
                info = await contract.functions._tokenInfos(token_cs).call()
                last_price = info[9]  
                if last_price > 0:
                    return (amount_token_raw * last_price) / 1e36
                return 0.0

            else:
                return 0.0
        except Exception:
            return 0.0

    async def get_token_balance(self, token: str, wallet_address: str, chain: str = "bsc") -> int:
        w3 = await self.rpc.get_http(chain)
        erc20 = w3.eth.contract(address=AsyncWeb3.to_checksum_address(token), abi=_ERC20_ABI)
        return await erc20.functions.balanceOf(AsyncWeb3.to_checksum_address(wallet_address)).call()