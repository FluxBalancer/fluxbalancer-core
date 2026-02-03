from typing import Protocol

from src.modules.routing.domain.policies.ranking_strategy import RankingStrategy


class BalancerStrategyProvider(Protocol):
    def get(self, name: str) -> RankingStrategy: ...
