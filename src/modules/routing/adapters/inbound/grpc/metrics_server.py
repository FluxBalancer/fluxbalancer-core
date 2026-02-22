import logging
from dataclasses import asdict

import grpc.aio
import contracts.metrics.metrics_pb2 as metrics_pb2
import contracts.metrics.metrics_pb2_grpc as metrics_pb2_grpc

from src.modules.observability.application.ports.metrics_repository import (
    MetricsRepository,
)
from src.modules.discovery.application.ports.node_registry import (
    NodeRegistry,
)
from src.modules.observability.domain.node_metrics import NodeMetrics

logger = logging.getLogger("metrics.service")


class MetricsService(metrics_pb2_grpc.MetricsServiceServicer):
    def __init__(self, repo: MetricsRepository, registry: NodeRegistry):
        self.metrics_repo = repo
        self.node_registry = registry

    async def PushMetrics(
        self, request: metrics_pb2.NodeMetrics, context: grpc.aio.ServicerContext
    ):
        peer: str = context.peer()
        ip: str = peer.split(":")[1]

        if ip and request.port:
            self.node_registry.update(
                node_id=request.node_id, host=ip, port=request.port
            )

        node_metric = NodeMetrics(
            timestamp=str(NodeMetrics.now()),
            node_id=request.node_id,
            cpu_util=float(request.cpu_util),
            mem_util=float(request.mem_util),
            net_in_bytes=int(request.net_in_bytes),
            net_out_bytes=int(request.net_out_bytes),
        )
        logger.info({"message": "get metrics", "metrics": {**asdict(node_metric)}})
        await self.metrics_repo.upsert(node_metric)

        return metrics_pb2.Ack(ok=True)


async def start_grpc_metrics_server(
    repo: MetricsRepository,
    registry: NodeRegistry,
    port: int = 50051,
) -> grpc.aio.Server:
    server = grpc.aio.server(
        options=[
            ("grpc.keepalive_time_ms", 10_000),
            ("grpc.keepalive_timeout_ms", 5_000),
            ("grpc.keepalive_permit_without_calls", 1),
            ("grpc.http2.max_pings_without_data", 0),
        ]
    )

    metrics_pb2_grpc.add_MetricsServiceServicer_to_server(
        MetricsService(repo, registry), server
    )
    server.add_insecure_port(f"[::]:{port}")

    await server.start()
    print(f"gRPC server started on port {port}")
    return server
