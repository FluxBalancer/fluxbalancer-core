import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager


class InflightTracker:
    """
    Отслеживает число запросов in-flight по node_id.

    Нужен для load-aware hedging:
    backup-реплику можно запускать только если целевая нода сейчас idle.
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self._counts: dict[str, int] = defaultdict(int)

    async def increment(self, node_id: str) -> None:
        async with self._lock:
            self._counts[node_id] += 1

    async def decrement(self, node_id: str) -> None:
        async with self._lock:
            current = self._counts.get(node_id, 0)
            if current <= 1:
                self._counts.pop(node_id, None)
            else:
                self._counts[node_id] = current - 1

    async def get(self, node_id: str) -> int:
        async with self._lock:
            return self._counts.get(node_id, 0)

    async def is_greater_than_limit(self, node_id: str, limit: int) -> bool:
        current = await self.get(node_id)
        return current > limit

    @asynccontextmanager
    async def track(self, node_id: str):
        await self.increment(node_id)
        try:
            yield
        finally:
            await self.decrement(node_id)
