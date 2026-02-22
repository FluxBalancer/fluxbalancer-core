from typing import Protocol


class CollectorManager(Protocol):
    async def collect(self) -> None: ...
