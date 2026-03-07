"""Asyncio task lifecycle manager."""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable


class RuntimeManager:
    def __init__(self):
        self._jobs: dict[str, asyncio.Task] = {}

    def start_job(self, key: str, coro_factory: Callable[[], Awaitable[None]]) -> None:
        if key in self._jobs and not self._jobs[key].done():
            return
        self._jobs[key] = asyncio.create_task(coro_factory())

    async def stop_job(self, key: str) -> None:
        task = self._jobs.get(key)
        if not task:
            return
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        self._jobs.pop(key, None)

    async def stop_all(self) -> None:
        for key in list(self._jobs.keys()):
            await self.stop_job(key)

    def is_running(self, key: str) -> bool:
        task = self._jobs.get(key)
        return task is not None and not task.done()
