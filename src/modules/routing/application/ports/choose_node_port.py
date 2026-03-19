from typing import Protocol

from modules.gateway.application.dto.brs import BRSRequest


class ChooseNodePort(Protocol):
    async def execute(
        self,
        brs: BRSRequest,
        request_profile: str | None = None,
    ) -> tuple[str, str, int]: ...
