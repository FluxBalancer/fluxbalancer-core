from src.modules.routing.domain.policies.metric_extractor_policy import (
    MetricExtractorPolicy,
)


class CpuExtractorPolicy(MetricExtractorPolicy):
    def extract(self, raw_stats: dict) -> dict[str, float]:
        cpu_delta = (
            raw_stats["cpu_stats"]["cpu_usage"]["total_usage"]
            - raw_stats["precpu_stats"]["cpu_usage"]["total_usage"]
        )
        system_delta = (
            raw_stats["cpu_stats"]["system_cpu_usage"]
            - raw_stats["precpu_stats"]["system_cpu_usage"]
        )
        online_cpus = raw_stats["cpu_stats"].get("online_cpus") or 1

        cpu_util = cpu_delta / (system_delta + 1e-12) * online_cpus
        return {"cpu_util": float(cpu_util)}
