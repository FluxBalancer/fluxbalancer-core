from typing import Protocol

from src.modules.decision.domain.ranking_strategy import RankingStrategy


class BalancerStrategyProvider(Protocol):
    def get(self, name: str) -> RankingStrategy: ...
