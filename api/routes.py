"""FastAPI route definitions.

All services are injected via module-level singletons created in main.py
and passed in through the `build_router` factory.
"""

from __future__ import annotations

import asyncio
import json

import aiosqlite
from fastapi import APIRouter, HTTPException, Query, WebSocket

import json as _json

from core.schemas import (
    CopyTaskCreateRequest,
    ListenerTaskCreateRequest,
    ListenerTaskRenameRequest,
    PanicSellRequest,
    RpcConfigCreateRequest,
    StrategyTaskCreateRequest,
    TaskStatusUpdateRequest,
    TransferRequest,
    WalletGenerateRequest,
    WalletRenameRequest,
)
from database.db import DB_PATH, fetch_all, fetch_one
from logs.service import LogService
from rpc.manager import RpcManager
from utils.runtime import RuntimeManager
from wallet.service import WalletService

# Lazy imports to avoid circular deps — engines are passed in at build time
from copytrade.engine import CopyTradeEngine
from listener.engine import ListenerEngine
from strategy.engine import StrategyEngine


def build_router(
    log_service: LogService,
    wallet_service: WalletService,
    rpc_manager: RpcManager,
    runtime: RuntimeManager,
    listener_engine: ListenerEngine,
    copy_engine: CopyTradeEngine,
    strategy_engine: StrategyEngine,
) -> APIRouter:
    router = APIRouter()

    # ------------------------------------------------------------------
    # Health / Dashboard
    # ------------------------------------------------------------------

    @router.get("/api/health")
    async def health():
        return {"status": "ok", "service": "copybot", "version": "1.0.0"}

    @router.get("/api/dashboard")
    async def dashboard():
        async with aiosqlite.connect(DB_PATH) as db:
            stats: dict = {}
            for table in (
                "wallets",
                "listener_tasks",
                "copy_tasks",
                "strategy_tasks",
                "transactions",
            ):
                cur = await db.execute(f"SELECT COUNT(1) FROM {table}")
                stats[table] = (await cur.fetchone())[0]

            cur = await db.execute(
                "SELECT COUNT(1) FROM listener_tasks WHERE status = 'running'"
            )
            stats["active_listeners"] = (await cur.fetchone())[0]

            cur = await db.execute(
                "SELECT COUNT(1) FROM copy_tasks WHERE status = 'running'"
            )
            stats["active_copies"] = (await cur.fetchone())[0]
            return stats

    @router.get("/api/listener-tasks/{task_id}")
    async def get_listener_task(task_id: int):
        row = await fetch_one("SELECT * FROM listener_tasks WHERE id = ?", (task_id,))
        if not row:
            raise HTTPException(status_code=404, detail="task not found")
        return row

    @router.get("/api/listener-tasks/{task_id}/events")
    async def listener_task_events(
        task_id: int,
        limit: int = Query(default=200, ge=1, le=1000),
        after_id: int = Query(default=0, ge=0),
    ):
        """Return detected trade events for a specific listener task.
        Use after_id for incremental polling (only rows with id > after_id).
        """
        if after_id:
            return await fetch_all(
                """SELECT * FROM transactions
                   WHERE source_task_id = ? AND source_task_type = 'listener' AND id > ?
                   ORDER BY id DESC LIMIT ?""",
                (task_id, after_id, limit),
            )
        return await fetch_all(
            """SELECT * FROM transactions
               WHERE source_task_id = ? AND source_task_type = 'listener'
               ORDER BY id DESC LIMIT ?""",
            (task_id, limit),
        )

    # ------------------------------------------------------------------
    # WebSocket log stream
    # ------------------------------------------------------------------

    @router.websocket("/ws/logs")
    async def websocket_logs(websocket: WebSocket):
        await log_service.connect(websocket)
        await websocket.send_json(
            {"level": "SYSTEM", "message": "connected to CopyBot node"}
        )
        try:
            while True:
                await websocket.receive_text()
        except Exception:
            await log_service.disconnect(websocket)

    # ------------------------------------------------------------------
    # Wallets
    # ------------------------------------------------------------------

    @router.post("/api/wallets/generate")
    async def generate_wallets(payload: WalletGenerateRequest):
        addresses = await wallet_service.create_wallets(payload.count)
        await log_service.push(
            f"generated {len(addresses)} wallet(s)", "SUCCESS", "wallet"
        )
        return {"status": "ok", "addresses": addresses}

    @router.get("/api/wallets")
    async def list_wallets():
        """Returns wallet list without balances. Fetch balance per-wallet via /balance."""
        return await wallet_service.list_wallets()

    @router.get("/api/wallets/{wallet_id}/balance")
    async def wallet_balance(wallet_id: int):
        """Lightweight single-wallet BNB balance — called on demand."""
        row = await fetch_one("SELECT address FROM wallets WHERE id = ?", (wallet_id,))
        if not row:
            raise HTTPException(status_code=404, detail="wallet not found")
        balance = await wallet_service.get_wallet_balance(row["address"])
        return {"balance": balance}

    @router.get("/api/wallet-balance")
    async def address_balance(address: str = Query(...)):
        """BNB balance for any address."""
        balance = await wallet_service.get_wallet_balance(address)
        return {"balance": balance}

    @router.patch("/api/wallets/{wallet_id}/name")
    async def rename_wallet(wallet_id: int, payload: WalletRenameRequest):
        ok = await wallet_service.update_name(wallet_id, payload.name)
        if not ok:
            raise HTTPException(status_code=404, detail="wallet not found")
        await log_service.push(
            f"wallet #{wallet_id} renamed to '{payload.name}'", "INFO", "wallet"
        )
        return {"status": "ok"}

    @router.get("/api/wallets/{wallet_id}/private-key")
    async def get_private_key(wallet_id: int):
        """Return the private key for wallet *wallet_id*. Frontend must confirm before calling."""
        try:
            pk = await wallet_service.get_private_key(wallet_id)
        except ValueError:
            raise HTTPException(status_code=404, detail="wallet not found")
        await log_service.push(
            f"private key accessed for wallet #{wallet_id}", "WARNING", "wallet"
        )
        return {"private_key": pk}

    @router.get("/api/wallets/{wallet_id}/tokens")
    async def wallet_tokens(wallet_id: int):
        """Return ERC-20 token holdings with BNB value for wallet *wallet_id*."""
        holdings = await wallet_service.get_token_holdings(wallet_id)
        return holdings

    @router.post("/api/wallets/{wallet_id}/transfer")
    async def transfer(wallet_id: int, payload: TransferRequest):
        to = wallet_service.validate_evm_address(payload.to_address)
        if not to:
            raise HTTPException(status_code=400, detail="invalid recipient address")
        result = await wallet_service.transfer(
            wallet_id, payload.to_address, payload.token, payload.amount
        )
        level = "SUCCESS" if result.get("status") == "submitted" else "ERROR"
        token_label = payload.token or "BNB"
        await log_service.push(
            f"transfer {result.get('status')}: {payload.amount} {token_label} → {to}",
            level,
            "wallet",
        )
        return result

    @router.post("/api/wallets/panic-sell")
    async def panic_sell(payload: PanicSellRequest):
        result = await wallet_service.panic_sell(
            payload.wallet_address, payload.token, payload.slippage
        )
        level = "SUCCESS" if result.get("status") == "submitted" else "WARNING"
        await log_service.push(
            f"panic sell {result.get('status')}: {payload.wallet_address} → {payload.token}",
            level,
            "copytrade",
        )
        return result

    # ------------------------------------------------------------------
    # Listener tasks
    # ------------------------------------------------------------------

    @router.post("/api/listener-tasks")
    async def create_listener_task(payload: ListenerTaskCreateRequest):
        target = wallet_service.validate_evm_address(payload.target_address)
        if not target:
            raise HTTPException(status_code=400, detail="invalid target_address")

        # Deduplicate platforms list
        platforms = list(dict.fromkeys(payload.platforms))

        async with aiosqlite.connect(DB_PATH) as db:
            # Enforce one listener config per target address
            cur = await db.execute(
                "SELECT id FROM listener_tasks WHERE target_address = ?", (target,)
            )
            existing = await cur.fetchone()
            if existing:
                raise HTTPException(
                    status_code=409,
                    detail=f"listener task already exists for {target} (id={existing[0]})",
                )

            try:
                cur = await db.execute(
                    """
                    INSERT INTO listener_tasks
                    (target_address, chain, platforms, label, status, config)
                    VALUES (?, ?, ?, ?, 'pending', ?)
                    """,
                    (
                        target,
                        payload.chain,
                        _json.dumps(platforms),
                        payload.label.strip(),
                        _json.dumps(payload.config),
                    ),
                )
                await db.commit()
                task_id = cur.lastrowid
            except Exception as exc:
                if "UNIQUE" in str(exc):
                    raise HTTPException(
                        status_code=409,
                        detail=f"listener task already exists for {target}",
                    )
                raise

        await log_service.push(
            f"listener task #{task_id} created for {target} [{', '.join(platforms)}]",
            "SUCCESS",
            "listener",
        )
        return {"status": "ok", "id": task_id}

    @router.patch("/api/listener-tasks/{task_id}/label")
    async def rename_listener_task(task_id: int, payload: ListenerTaskRenameRequest):
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "UPDATE listener_tasks SET label = ? WHERE id = ?",
                (payload.label.strip(), task_id),
            )
            await db.commit()
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="task not found")
        return {"status": "ok"}

    @router.patch("/api/listener-tasks/{task_id}/platforms")
    async def update_listener_platforms(task_id: int, payload: dict):
        """Update which platforms a listener task monitors."""
        platforms = payload.get("platforms", [])
        if not platforms:
            raise HTTPException(status_code=400, detail="platforms cannot be empty")
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "UPDATE listener_tasks SET platforms = ? WHERE id = ?",
                (_json.dumps(list(dict.fromkeys(platforms))), task_id),
            )
            await db.commit()
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="task not found")
        return {"status": "ok"}

    @router.get("/api/listener-tasks")
    async def list_listener_tasks():
        return await fetch_all("SELECT * FROM listener_tasks ORDER BY id DESC")

    @router.patch("/api/listener-tasks/{task_id}/status")
    async def update_listener_status(task_id: int, payload: TaskStatusUpdateRequest):
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "UPDATE listener_tasks SET status = ? WHERE id = ?",
                (payload.status, task_id),
            )
            await db.commit()
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="task not found")

        key = f"listener:{task_id}"
        if payload.status == "running":
            runtime.start_job(
                key, lambda: listener_engine.run_listener_task(task_id)
            )
            await log_service.push(
                f"listener #{task_id} started", "INFO", "listener"
            )
        elif payload.status in {"paused", "interrupted", "pending"}:
            await runtime.stop_job(key)

        return {"status": "ok", "task_id": task_id, "new_status": payload.status}

    @router.delete("/api/listener-tasks/{task_id}")
    async def delete_listener_task(task_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            # Check task exists and is not running
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                "SELECT id, status FROM listener_tasks WHERE id = ?", (task_id,)
            )
            row = await cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="task not found")
            if row["status"] == "running":
                raise HTTPException(status_code=400, detail="stop the listener before deleting")

            cur2 = await db.execute(
                "SELECT COUNT(1) FROM copy_tasks WHERE listener_task_id = ?",
                (task_id,),
            )
            if (await cur2.fetchone())[0] > 0:
                raise HTTPException(
                    status_code=400,
                    detail="cannot delete: copy trade task is linked to this listener's target",
                )

            # Delete related transactions and the task itself
            await db.execute(
                "DELETE FROM transactions WHERE source_task_id = ? AND source_task_type = 'listener'",
                (task_id,),
            )
            await db.execute("DELETE FROM listener_tasks WHERE id = ?", (task_id,))
            await db.commit()

        await log_service.push(
            f"listener #{task_id} deleted", "WARNING", "listener"
        )
        return {"status": "ok", "task_id": task_id}

    # ------------------------------------------------------------------
    # Copy tasks
    # ------------------------------------------------------------------

    @router.post("/api/copy-tasks")
    async def create_copy_task(payload: CopyTaskCreateRequest):
        # Validate listener task exists
        lt = await fetch_one(
            "SELECT id, target_address FROM listener_tasks WHERE id = ?",
            (payload.listener_task_id,),
        )
        if not lt:
            raise HTTPException(status_code=404, detail="listener task not found")

        # Validate wallet exists
        w = await fetch_one("SELECT id FROM wallets WHERE id = ?", (payload.wallet_id,))
        if not w:
            raise HTTPException(status_code=404, detail="wallet not found")

        # One wallet can only have one active copy task
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "SELECT id FROM copy_tasks WHERE wallet_id = ? AND status != 'deleted'",
                (payload.wallet_id,),
            )
            if await cur.fetchone():
                raise HTTPException(
                    status_code=409,
                    detail=f"wallet #{payload.wallet_id} already has a copy task",
                )

            # Derive buy_value for backward compat (use first condition or fixed amount)
            if payload.buy_mode == "fixed":
                buy_value = payload.buy_config.get("amount", 0.1)
            else:
                conds = payload.buy_config.get("conditions", [])
                buy_value = conds[0]["amount"] if conds else 0.1

            cur = await db.execute(
                """INSERT INTO copy_tasks
                   (target_address, wallet_id, listener_task_id,
                    buy_mode, buy_value, buy_config,
                    sell_mode, sell_config,
                    gas_multiplier, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')""",
                (
                    lt["target_address"],
                    payload.wallet_id,
                    payload.listener_task_id,
                    payload.buy_mode,
                    buy_value,
                    json.dumps(payload.buy_config),
                    payload.sell_mode,
                    json.dumps(payload.sell_config),
                    payload.gas_multiplier,
                ),
            )
            await db.commit()
            task_id = cur.lastrowid

        await log_service.push(
            f"copy task #{task_id} created — wallet #{payload.wallet_id} ← "
            f"listener #{payload.listener_task_id}",
            "SUCCESS", "copytrade",
        )
        return {"status": "ok", "id": task_id}

    @router.get("/api/copy-tasks")
    async def list_copy_tasks():
        return await fetch_all(
            """SELECT ct.*,
                      lt.label AS listener_label,
                      w.label AS wallet_label, w.address AS wallet_address
               FROM copy_tasks ct
               LEFT JOIN listener_tasks lt ON ct.listener_task_id = lt.id
               LEFT JOIN wallets w ON ct.wallet_id = w.id
               ORDER BY ct.id DESC"""
        )

    @router.patch("/api/copy-tasks/{task_id}/status")
    async def update_copy_status(task_id: int, payload: TaskStatusUpdateRequest):
        row = await fetch_one("SELECT * FROM copy_tasks WHERE id = ?", (task_id,))
        if not row:
            raise HTTPException(status_code=404, detail="task not found")

        # When starting, verify linked listener is running
        if payload.status == "running":
            lt = await fetch_one(
                "SELECT status FROM listener_tasks WHERE id = ?",
                (row["listener_task_id"],),
            )
            if not lt or lt["status"] != "running":
                raise HTTPException(
                    status_code=400,
                    detail="linked listener must be running first",
                )

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE copy_tasks SET status = ? WHERE id = ?",
                (payload.status, task_id),
            )
            await db.commit()

        key = f"copy:{task_id}"
        if payload.status == "running":
            runtime.start_job(key, lambda tid=task_id: copy_engine.run_copy_task(tid))
            await log_service.push(
                f"copy task #{task_id} started", "INFO", "copytrade",
            )
        elif payload.status in {"paused", "interrupted", "pending"}:
            await runtime.stop_job(key)

        return {"status": "ok", "task_id": task_id, "new_status": payload.status}

    @router.delete("/api/copy-tasks/{task_id}")
    async def delete_copy_task(task_id: int):
        row = await fetch_one(
            "SELECT id, status FROM copy_tasks WHERE id = ?", (task_id,),
        )
        if not row:
            raise HTTPException(status_code=404, detail="task not found")
        if row["status"] == "running":
            raise HTTPException(status_code=400, detail="stop the task before deleting")

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM copy_positions WHERE copy_task_id = ?", (task_id,),
            )
            await db.execute("DELETE FROM copy_tasks WHERE id = ?", (task_id,))
            await db.commit()

        await log_service.push(
            f"copy task #{task_id} deleted", "WARNING", "copytrade",
        )
        return {"status": "ok"}

    @router.get("/api/copy-tasks/{task_id}/positions")
    async def copy_task_positions(task_id: int):
        return await fetch_all(
            "SELECT * FROM copy_positions WHERE copy_task_id = ? ORDER BY id DESC",
            (task_id,),
        )

    @router.get("/api/copy-tasks/{task_id}/records")
    async def copy_task_records(task_id: int):
        """Return buy/sell transaction records for a copy task, with P&L from positions."""
        return await fetch_all(
            """SELECT t.*,
                      cp.id AS position_id,
                      cp.status AS position_status,
                      cp.profit_bnb AS pos_profit_bnb,
                      cp.profit_pct AS pos_profit_pct,
                      cp.amount_bnb AS pos_entry_bnb,
                      cp.sell_amount_bnb AS pos_sell_bnb,
                      cp.sell_reason AS pos_sell_reason
               FROM transactions t
               LEFT JOIN copy_positions cp ON cp.copy_task_id = t.source_task_id AND (
                   (t.action = 'buy' AND cp.buy_tx_hash = t.tx_hash) OR
                   (t.action = 'sell' AND cp.sell_tx_hash = t.tx_hash)
               )
               WHERE t.source_task_id = ? AND t.source_task_type = 'copy'
               ORDER BY t.id DESC
               LIMIT 200""",
            (task_id,),
        )

    @router.post("/api/copy-positions/{position_id}/sell")
    async def manual_sell_position(position_id: int):
        """Manually sell an open copy trade position."""
        try:
            result = await copy_engine.manual_sell(position_id)
            return {"status": "ok", **result}
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    @router.get("/api/copy-positions")
    async def all_copy_positions():
        return await fetch_all(
            """SELECT cp.*,
                      ct.listener_task_id,
                      lt.label AS listener_label,
                      lt.target_address,
                      w.label AS wallet_label,
                      w.address AS wallet_address
               FROM copy_positions cp
               JOIN copy_tasks ct ON cp.copy_task_id = ct.id
               LEFT JOIN listener_tasks lt ON ct.listener_task_id = lt.id
               LEFT JOIN wallets w ON ct.wallet_id = w.id
               ORDER BY cp.id DESC
               LIMIT 200"""
        )

    # ------------------------------------------------------------------
    # Strategy tasks
    # ------------------------------------------------------------------

    @router.post("/api/strategy-tasks")
    async def create_strategy_task(payload: StrategyTaskCreateRequest):
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                """
                INSERT INTO strategy_tasks
                (wallet_id, token, take_profit, stop_loss, status, config)
                VALUES (?, ?, ?, ?, 'pending', ?)
                """,
                (
                    payload.wallet_id,
                    payload.token,
                    payload.take_profit,
                    payload.stop_loss,
                    json.dumps(payload.config),
                ),
            )
            await db.commit()
            task_id = cur.lastrowid

        await log_service.push(
            f"strategy task #{task_id} created for wallet #{payload.wallet_id}",
            "SUCCESS",
            "strategy",
        )
        return {"status": "ok", "id": task_id}

    @router.get("/api/strategy-tasks")
    async def list_strategy_tasks():
        return await fetch_all("SELECT * FROM strategy_tasks ORDER BY id DESC")

    @router.patch("/api/strategy-tasks/{task_id}/status")
    async def update_strategy_status(task_id: int, payload: TaskStatusUpdateRequest):
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "UPDATE strategy_tasks SET status = ? WHERE id = ?",
                (payload.status, task_id),
            )
            await db.commit()
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail="task not found")

        key = f"strategy:{task_id}"
        if payload.status == "running":
            runtime.start_job(
                key, lambda: strategy_engine.run_strategy_task(task_id)
            )
            await log_service.push(
                f"strategy #{task_id} started", "INFO", "strategy"
            )
        elif payload.status in {"paused", "interrupted", "pending"}:
            await runtime.stop_job(key)

        await log_service.push(
            f"strategy task #{task_id} → {payload.status}", "INFO", "strategy"
        )
        return {"status": "ok", "task_id": task_id, "new_status": payload.status}

    # ------------------------------------------------------------------
    # Transactions & Logs
    # ------------------------------------------------------------------

    @router.get("/api/transactions")
    async def list_transactions(limit: int = Query(default=100, ge=1, le=1000)):
        return await fetch_all(
            """
            SELECT t.*, lt.label AS listener_label, lt.target_address AS listener_target
            FROM transactions t
            LEFT JOIN listener_tasks lt
                ON t.source_task_type = 'listener' AND lt.id = t.source_task_id
            ORDER BY t.id DESC LIMIT ?
            """,
            (limit,),
        )

    @router.get("/api/token-name")
    async def get_token_name(address: str = Query(...)):
        """Return ERC-20 name and symbol for a token address, with DB caching."""
        from web3 import AsyncWeb3
        addr_lower = address.lower()
        cached = await fetch_one(
            "SELECT name, symbol FROM token_cache WHERE address = ?", (addr_lower,)
        )
        if cached:
            return cached
        try:
            w3 = await rpc_manager.get_http("bsc")
            abi = [
                {"name": "name",   "outputs": [{"type": "string"}], "inputs": [], "type": "function", "stateMutability": "view"},
                {"name": "symbol", "outputs": [{"type": "string"}], "inputs": [], "type": "function", "stateMutability": "view"},
            ]
            checksum = AsyncWeb3.to_checksum_address(address)
            contract = w3.eth.contract(address=checksum, abi=abi)
            name, symbol = await asyncio.gather(
                contract.functions.name().call(),
                contract.functions.symbol().call(),
            )
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT OR REPLACE INTO token_cache (address, name, symbol) VALUES (?, ?, ?)",
                    (addr_lower, name, symbol),
                )
                await db.commit()
            return {"name": name, "symbol": symbol}
        except Exception:
            return {"name": "", "symbol": ""}

    @router.get("/api/logs")
    async def get_logs(limit: int = Query(default=200, ge=1, le=1000)):
        return await log_service.recent_logs(limit=limit)

    # ------------------------------------------------------------------
    # RPC config
    # ------------------------------------------------------------------

    @router.get("/api/rpc-configs")
    async def list_rpc_configs():
        return await fetch_all("SELECT * FROM rpc_configs ORDER BY active DESC, id DESC")

    @router.post("/api/rpc-configs")
    async def create_rpc_config(payload: RpcConfigCreateRequest):
        async with aiosqlite.connect(DB_PATH) as db:
            # Check if any active config exists for the chain
            cur = await db.execute(
                "SELECT COUNT(1) FROM rpc_configs WHERE chain = ? AND active = 1",
                (payload.chain,),
            )
            has_active = (await cur.fetchone())[0] > 0
            # New RPC is active only if no other active config exists
            active = 0 if has_active else 1
            cur = await db.execute(
                """
                INSERT INTO rpc_configs (chain, label, rpc_url, ws_url, chain_id, active)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (payload.chain, payload.label, payload.rpc_url, payload.ws_url, payload.chain_id, active),
            )
            await db.commit()
            new_id = cur.lastrowid
        if active:
            rpc_manager.invalidate(payload.chain)
        await log_service.push(
            f"RPC endpoint added (id={new_id})", "INFO", "system"
        )
        return {"status": "ok", "id": new_id}

    @router.patch("/api/rpc-configs/{config_id}/activate")
    async def activate_rpc_config(config_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            # Get the chain for this config
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT chain FROM rpc_configs WHERE id = ?", (config_id,))
            row = await cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="RPC config not found")
            chain = row["chain"]
            # Deactivate all configs for this chain
            await db.execute(
                "UPDATE rpc_configs SET active = 0 WHERE chain = ?", (chain,)
            )
            # Activate the selected one
            await db.execute(
                "UPDATE rpc_configs SET active = 1 WHERE id = ?", (config_id,)
            )
            await db.commit()
        rpc_manager.invalidate(chain)
        # Restart all running listener tasks so they pick up the new RPC
        running_listeners = await fetch_all(
            "SELECT id FROM listener_tasks WHERE chain = ? AND status = 'running'",
            (chain,),
        )
        for t in running_listeners:
            key = f"listener:{t['id']}"
            await runtime.stop_job(key)
            runtime.start_job(key, lambda tid=t["id"]: listener_engine.run_listener_task(tid))
        await log_service.push(
            f"RPC switched to config #{config_id}, restarted {len(running_listeners)} listener(s)",
            "INFO", "system",
        )
        return {"status": "ok"}

    @router.delete("/api/rpc-configs/{config_id}")
    async def delete_rpc_config(config_id: int):
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute("SELECT active, is_default FROM rpc_configs WHERE id = ?", (config_id,))
            row = await cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="RPC config not found")
            if row["is_default"]:
                raise HTTPException(status_code=400, detail="Cannot delete the built-in default RPC")
            if row["active"]:
                raise HTTPException(status_code=400, detail="Cannot delete the active RPC config")
            await db.execute("DELETE FROM rpc_configs WHERE id = ?", (config_id,))
            await db.commit()
        return {"status": "ok"}

    return router
