from typing import Protocol

from src.modules.decision.application.ports.weights_provider import (
    WeightsProvider,
)


class WeightStrategyProvider(Protocol):
    def get(self, name: str) -> WeightsProvider: ...
