import numpy as np

from src.modules.decision.adapters.algorithms.saw import saw
from src.modules.decision.domain.ranking_strategy import RankingStrategy
from src.modules.types.numpy import Matrix, Vector


class SAWStrategy(RankingStrategy):
    def choose(self, scores: Matrix, weights: Vector) -> int:
        values = self.score_all(scores, weights)
        return int(np.argmax(values))

    def score_all(self, scores: Matrix, weights: Vector) -> Vector:
        return saw(scores, weights)
