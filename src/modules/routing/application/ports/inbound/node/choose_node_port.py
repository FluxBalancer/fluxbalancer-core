from typing import Protocol

from src.modules.routing.application.dto.brs import BRSRequest
from src.modules.routing.domain.policies.ranking_strategy import RankingStrategy


class ChooseNodePort(Protocol):
    async def execute(self, brs: BRSRequest) -> tuple[str, str, int]:
        """
        Returns:
            (node_id, host, port)
        """
        ...
