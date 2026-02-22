from src.modules.observability.application.ports.metric_extractor_policy import (
    MetricExtractorPolicy,
)


class MemoryExtractorPolicy(MetricExtractorPolicy):
    def extract(self, raw_stats: dict) -> dict[str, float]:
        mem = raw_stats["memory_stats"]
        mem_util = mem["usage"] / (mem["limit"] + 1e-12)
        return {"mem_util": float(mem_util)}
