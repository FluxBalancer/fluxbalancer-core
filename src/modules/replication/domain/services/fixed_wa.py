from modules.replication.domain.policies.wa_estimator import WAEstimator


class FixedWAEstimator(WAEstimator):
    """WA для fixed r-way: ΔWA_r = 1."""

    def delta_wa(self, r: int) -> float:
        return 1.0
