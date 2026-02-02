from typing import Protocol

from src.modules.routing.domain.policies.ranking_strategy import RankingStrategy


class StrategyProvider(Protocol):
    def get(self, name: str) -> RankingStrategy: ...
