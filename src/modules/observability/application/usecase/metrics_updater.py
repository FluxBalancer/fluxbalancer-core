import asyncio

from src.modules.observability.application.ports.collector import (
    CollectorManager,
)


class MetricsUpdater:
    """
    Application-сервис, который решает КОГДА собирать метрики.
    """

    def __init__(self, collector: CollectorManager, collector_interval: float):
        self.collector = collector
        self.period = collector_interval
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run_forever())

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except Exception as e:
                pass

    async def _run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                await self.collector.collect()
            except Exception as e:
                print("metrics collection failed:", repr(e))

            await asyncio.sleep(self.period)
