from enum import StrEnum

from modules.decision.adapters.outbound.weights.weights_provider import (
    EntropyWeightsProvider,
)
from modules.decision.application.ports.outbound.strategy_provider import (
    StrategyProvider,
)
from modules.decision.domain.weights_strategy import WeightsStrategy


class WeightsAlgorithmName(StrEnum):
    ENTROPY = "entropy"
    FIXED = "fixed"


class WeightsProviderRegistry(StrategyProvider[WeightsStrategy]):
    def __init__(self):
        self._providers: dict[str, WeightsStrategy] = {
            WeightsAlgorithmName.ENTROPY: EntropyWeightsProvider(),
        }

    def get(self, name: str) -> WeightsStrategy:
        try:
            key = WeightsAlgorithmName(name.strip().lower())
            return self._providers[key]
        except (Exception, KeyError) as e:
            raise ValueError(f"Неизвестный алгоритм весов: {name}") from e
