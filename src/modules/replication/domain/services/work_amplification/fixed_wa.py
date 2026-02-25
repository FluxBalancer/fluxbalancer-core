from modules.replication.domain.policies.wa_estimator import WAEstimator


class FixedWAEstimator(WAEstimator):
    """WA для fixed r-way: ΔWA_r = 1."""

    def delta_wa(self, *, delay_ms: float, prev_finish_hat_ms: float) -> float:
        return 1.0
