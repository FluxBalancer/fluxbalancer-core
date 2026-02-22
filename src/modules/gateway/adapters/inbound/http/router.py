from fastapi import APIRouter
from starlette.responses import JSONResponse

from src.modules.observability.application.ports.metrics_aggregation_repository import (
    MetricsAggregationRepository,
)


class ChooseNodeRouter:
    def __init__(
        self,
        metrics_agg_repo: MetricsAggregationRepository,
    ):
        self.router = APIRouter()

        @self.router.get("/stats")
        async def stats():
            """Сводные метрики по задержкам и ресурсам."""
            data = metrics_agg_repo.get_averages()
            return JSONResponse(data)
