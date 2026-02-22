import numpy as np

from modules.decision.domain.services.mcdm.topsis import topsis
from src.modules.decision.domain.ranking_strategy import RankingStrategy
from src.modules.types.numpy import Matrix, Vector


class TopsisStrategy(RankingStrategy):
    def choose(self, scores: Matrix, weights: Vector) -> int:
        values = self.score_all(scores, weights)
        return int(np.argmax(values))

    def score_all(self, scores: Matrix, weights: Vector) -> Vector:
        return topsis(scores, weights)
