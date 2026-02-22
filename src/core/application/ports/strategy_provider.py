from typing import Protocol, Any


class StrategyProvider[Provider](Protocol):
    def get(self, name: str | None, **kwargs: Any) -> Provider: ...
