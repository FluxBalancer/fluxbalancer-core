from typing import Protocol

from src.modules.routing.application.dto.brs import BRSRequest


class ChooseNodePort(Protocol):
    async def execute(self, brs: BRSRequest) -> tuple[str, str, int]:
        """
        Returns:
            (node_id, host, port)
        """
        ...
