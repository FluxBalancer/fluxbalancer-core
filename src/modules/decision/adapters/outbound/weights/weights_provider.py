from modules.decision.domain.services.entropy import entropy_weights
from modules.decision.domain.weights_strategy import WeightsStrategy
from src.modules.types.numpy import Matrix, Vector


class EntropyWeightsProvider(WeightsStrategy):
    def compute(self, matrix: Matrix) -> Vector:
        return entropy_weights(matrix)
