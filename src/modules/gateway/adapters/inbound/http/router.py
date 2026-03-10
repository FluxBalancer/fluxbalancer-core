from fastapi import APIRouter
from starlette.responses import JSONResponse

from modules.observability.application.ports.metrics_repository import MetricsRepository
from src.modules.observability.application.ports.metrics_aggregation_repository import (
    MetricsAggregationRepository,
)


class ChooseNodeRouter:
    def __init__(
        self,
        metrics_agg_repo: MetricsAggregationRepository,
        metrics_repo: MetricsRepository
    ):
        self.router = APIRouter()

        @self.router.get("/stats")
        async def stats():
            """Сводные метрики по задержкам и ресурсам."""
            data = metrics_agg_repo.get_averages()
            return JSONResponse(data)

        @self.router.get("/clear")
        async def clear():
            """Сводные метрики по задержкам и ресурсам."""
            await metrics_repo.clear()
            return JSONResponse(True)