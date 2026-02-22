import numpy as np

from modules.decision.domain.services.mcdm.airm import airm
from src.modules.decision.domain.ranking_strategy import RankingStrategy
from src.modules.types.numpy import Matrix, Vector


class AIRMStrategy(RankingStrategy):
    def choose(self, scores: Matrix, weights: Vector) -> int:
        values = self.score_all(scores, weights)
        return int(np.argmax(values))

    def score_all(self, scores: Matrix, weights: Vector) -> Vector:
        return airm(scores, weights)
