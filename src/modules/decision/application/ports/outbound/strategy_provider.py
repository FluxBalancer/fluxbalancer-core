from typing import Protocol


class StrategyProvider[Provider](Protocol):
    def get(self, name: str) -> Provider: ...
