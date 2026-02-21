import grpc.aio
from contracts.metrics import metrics_pb2, metrics_pb2_grpc

from src.modules.routing.application.ports.outbound.metrics.metrics_repository import MetricsRepository
from src.modules.routing.application.ports.outbound.node.node_registry import NodeRegistry
from src.modules.routing.domain.entities.node.node_metrics import NodeMetrics


class MetricsService(metrics_pb2_grpc.MetricsServiceServicer):
    def __init__(
            self,
            repo: MetricsRepository,
            registry: NodeRegistry
    ):
        self.metrics_repo = repo
        self.node_registry = registry

    async def PushMetrics(self, request: metrics_pb2.NodeMetricsBatch, context: grpc.aio.ServicerContext):
        for m in request.items:
            m: metrics_pb2.NodeMetrics

            if m.host and m.port:
                self.node_registry.update(
                    node_id=m.node_id,
                    host=m.host,
                    port=m.port
                )

            node_metric = NodeMetrics(
                timestamp=str(m.timestamp_unix_ms),
                node_id=m.node_id,
                cpu_util=float(m.cpu_util),
                mem_util=float(m.mem_util),
                net_in_bytes=int(m.net_in_bytes),
                net_out_bytes=int(m.net_out_bytes),
                latency_ms=float(m.latency_ms) if m.latency_ms else None,
            )
            await self.metrics_repo.upsert(node_metric)

        return metrics_pb2.Ack(ok=True)


async def start_grpc_metrics_server(
        repo: MetricsRepository,
        registry: NodeRegistry,
        host: str = "0.0.0.0",
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
    server.add_insecure_port(f"{host}:{port}")

    await server.start()
    return server
