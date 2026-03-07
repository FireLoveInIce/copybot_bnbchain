"""Log service: WebSocket broadcast + SQLite persistence."""

import asyncio
import json

import aiosqlite
from fastapi import WebSocket

from database.db import DB_PATH


class LogService:
    def __init__(self):
        self.clients: set[WebSocket] = set()
        self._lock = asyncio.Lock()

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self.clients.add(websocket)

    async def disconnect(self, websocket: WebSocket) -> None:
        async with self._lock:
            self.clients.discard(websocket)

    async def push(
        self,
        message: str,
        level: str = "INFO",
        category: str = "system",
        tx_hash: str | None = None,
        task_id: int | None = None,
    ) -> None:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO logs (level, category, message, tx_hash) VALUES (?, ?, ?, ?)",
                (level, category, message, tx_hash),
            )
            await db.commit()

        payload = json.dumps(
            {"level": level, "category": category, "message": message,
             "tx_hash": tx_hash, "task_id": task_id}
        )

        if not self.clients:
            return

        dead: list[WebSocket] = []
        for client in self.clients:
            try:
                await client.send_text(payload)
            except Exception:
                dead.append(client)

        if dead:
            async with self._lock:
                for client in dead:
                    self.clients.discard(client)

    async def recent_logs(self, limit: int = 200) -> list[dict]:
        async with aiosqlite.connect(DB_PATH) as db:
            db.row_factory = aiosqlite.Row
            cur = await db.execute(
                """
                SELECT id, timestamp, level, category, message, tx_hash
                FROM logs
                ORDER BY id DESC
                LIMIT ?
                """,
                (limit,),
            )
            rows = await cur.fetchall()
            return [dict(row) for row in rows]
