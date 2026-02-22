from enum import StrEnum

from src.modules.routing.adapters.outbound.weights.weights_provider import (
    EntropyWeightsProvider,
)
from src.modules.routing.application.ports.outbound.strategy.weight_strategy_provider import (
    WeightStrategyProvider,
)
from src.modules.routing.application.ports.outbound.weights.weights_provider import (
    WeightsProvider,
)


class WeightsAlgorithmName(StrEnum):
    ENTROPY = "entropy"
    FIXED = "fixed"


class WeightsProviderRegistry(WeightStrategyProvider):
    def __init__(self):
        self._providers: dict[str, WeightsProvider] = {
            WeightsAlgorithmName.ENTROPY: EntropyWeightsProvider(),
        }

    def get(self, name: str) -> WeightsProvider:
        try:
            key = WeightsAlgorithmName(name.strip().lower())
            return self._providers[key]
        except (Exception, KeyError) as e:
            raise ValueError(f"Неизвестный алгоритм весов: {name}") from e
