from src.modules.decision.adapters.weights import entropy_weights
from src.modules.decision.application.ports.weights_provider import (
    WeightsProvider,
)
from src.modules.types.numpy import Matrix, Vector


class EntropyWeightsProvider(WeightsProvider):
    def compute(self, matrix: Matrix) -> Vector:
        return entropy_weights(matrix)
