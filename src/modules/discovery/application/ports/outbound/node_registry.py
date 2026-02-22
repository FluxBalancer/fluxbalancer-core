from typing import Protocol


class NodeRegistry(Protocol):
    def get_endpoint(self, node_id: str) -> tuple[str, int]: ...

    def update(self, node_id: str, host: str, port: int) -> None: ...
