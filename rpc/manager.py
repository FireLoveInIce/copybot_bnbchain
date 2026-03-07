"""RPC connection manager.

Loads the active RPC config for a chain from the database and returns
ready-to-use AsyncWeb3 instances for both HTTP and WebSocket providers.
"""

from __future__ import annotations

import aiosqlite
from web3 import AsyncWeb3
from web3.middleware import async_geth_poa_middleware

from core.constants import DEFAULT_CHAIN_ID, DEFAULT_RPC_URL, DEFAULT_WS_URL
from database.db import DB_PATH


class RpcManager:
    """Cache and provide Web3 connections per chain."""

    def __init__(self):
        self._http_cache: dict[str, AsyncWeb3] = {}
        self._config_cache: dict[str, dict] = {}

    async def get_config(self, chain: str = "bsc") -> dict:
        """Return the active RPC config for *chain* from DB, with fallback."""
        if chain in self._config_cache:
            return self._config_cache[chain]

        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT * FROM rpc_configs WHERE chain = ? AND active = 1",
                (chain,),
            )
            row = await cur.fetchone()

        if row:
            cfg = dict(row)
        else:
            cfg = {
                "chain": chain,
                "rpc_url": DEFAULT_RPC_URL,
                "ws_url": DEFAULT_WS_URL,
                "chain_id": DEFAULT_CHAIN_ID,
            }

        self._config_cache[chain] = cfg
        return cfg

    def invalidate(self, chain: str = "bsc") -> None:
        """Call after updating rpc_configs so the cache is refreshed."""
        self._config_cache.pop(chain, None)
        self._http_cache.pop(chain, None)

    async def get_http(self, chain: str = "bsc") -> AsyncWeb3:
        if chain not in self._http_cache:
            cfg = await self.get_config(chain)
            w3 = AsyncWeb3(AsyncWeb3.AsyncHTTPProvider(cfg["rpc_url"]))
            # BSC is a POA chain — inject async middleware to handle oversized extraData in block headers.
            w3.middleware_onion.inject(async_geth_poa_middleware, layer=0)
            self._http_cache[chain] = w3
        return self._http_cache[chain]

    async def get_ws_url(self, chain: str = "bsc") -> str:
        """Return configured WSS URL, falling back to DEFAULT_WS_URL."""
        cfg = await self.get_config(chain)
        return (cfg.get("ws_url") or DEFAULT_WS_URL).strip()
