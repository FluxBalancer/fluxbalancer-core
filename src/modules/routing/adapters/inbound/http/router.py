from fastapi import APIRouter
from starlette.responses import JSONResponse

from src.modules.routing.application.ports.inbound.node.choose_node_port import (
    ChooseNodePort,
)
from src.modules.routing.application.ports.outbound.metrics.metrics_aggregation_repository import (
    MetricsAggregationRepository,
)


class ChooseNodeRouter:
    def __init__(
        self,
        choose_node: ChooseNodePort,
        metrics_agg_repo: MetricsAggregationRepository,
    ):
        self.router = APIRouter()

        @self.router.get("/stats")
        async def stats():
            """Сводные метрики по задержкам и ресурсам."""
            data = metrics_agg_repo.get_averages()
            return JSONResponse(data)
