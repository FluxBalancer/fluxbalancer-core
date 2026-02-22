from threading import RLock

from modules.discovery.application.ports.outbound.node_registry import NodeRegistry


class InMemoryNodeRegistry(NodeRegistry):
    def __init__(self):
        self._lock = RLock()
        self._endpoints: dict[str, tuple[str, int]] = {}

    def update(self, node_id: str, host: str, port: int) -> None:
        with self._lock:
            self._endpoints[node_id] = (host, int(port))

    def get_endpoint(self, node_id: str) -> tuple[str, int]:
        with self._lock:
            return self._endpoints[node_id]
