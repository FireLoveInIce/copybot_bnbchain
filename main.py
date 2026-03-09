"""CopyBot — entry point.

Start with:
    python main.py
or:
    uvicorn main:app --host 0.0.0.0 --port 8000 --reload
"""

import asyncio
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# Ensure project root is on sys.path so sub-packages resolve correctly.
import os
sys.path.insert(0, os.path.dirname(__file__))

from api.routes import build_router
from copytrade.engine import CopyTradeEngine
from database.db import init_db, fetch_all
from listener.engine import ListenerEngine
from logs.service import LogService
from rpc.manager import RpcManager
from strategy.engine import StrategyEngine
from utils.runtime import RuntimeManager
from wallet.service import WalletService

# ---------------------------------------------------------------------------
# Singletons
# ---------------------------------------------------------------------------
log_service = LogService()
rpc_manager = RpcManager()
wallet_service = WalletService(rpc_manager)
runtime = RuntimeManager()

listener_engine = ListenerEngine(log_service, rpc_manager)
copy_engine = CopyTradeEngine(log_service, rpc_manager, listener_engine)
strategy_engine = StrategyEngine(log_service, rpc_manager)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(_: FastAPI):
    await init_db()

    # Mark any tasks that were "running" when the server last died
    recovered = await _recover_tasks()
    if any(recovered.values()):
        await log_service.push(
            f"recovered interrupted tasks: {recovered}", "WARNING", "system"
        )

    await log_service.push("CopyBot started", "SYSTEM", "system")

    try:
        yield
    finally:
        await runtime.stop_all()
        await log_service.push("CopyBot stopped", "SYSTEM", "system")


async def _recover_tasks() -> dict[str, int]:
    import aiosqlite
    from database.db import DB_PATH

    counts: dict[str, int] = {}
    async with aiosqlite.connect(DB_PATH) as db:
        for table in ("listener_tasks", "copy_tasks", "strategy_tasks"):
            cur = await db.execute(
                f"UPDATE {table} SET status = 'interrupted' WHERE status = 'running'"
            )
            counts[table] = cur.rowcount
        await db.commit()
    return counts


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

app = FastAPI(title="CopyBot API", version="1.0.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static"), name="static")

router = build_router(
    log_service=log_service,
    wallet_service=wallet_service,
    rpc_manager=rpc_manager,
    runtime=runtime,
    listener_engine=listener_engine,
    copy_engine=copy_engine,
    strategy_engine=strategy_engine,
)
app.include_router(router)


@app.get("/")
async def index():
    return FileResponse("static/index.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, log_level="info")
