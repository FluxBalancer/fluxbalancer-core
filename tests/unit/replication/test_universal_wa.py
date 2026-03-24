import math

from modules.replication.domain.services.work_amplification.universal_wa import (
    UniversalWAEstimator,
)


def test_delta_wa_returns_one_for_non_positive_delay():
    estimator = UniversalWAEstimator(latency_samples_per_node=[[10, 20, 30]])

    value = estimator.delta_wa(
        delay_ms=0,
        prev_finish_hat_ms=50,
        active_prefix=1,
        delays_ms=[0],
    )

    assert value == 1.0


def test_delta_wa_uses_empirical_survival_product():
    estimator = UniversalWAEstimator(
        latency_samples_per_node=[
            [10, 20, 30, 40],
            [50, 60, 70, 80],
        ]
    )

    value = estimator.delta_wa(
        delay_ms=25,
        prev_finish_hat_ms=100,
        active_prefix=1,
        delays_ms=[0, 25],
    )

    # Для первой ноды S(25) = P(T > 25) = 2/4 = 0.5
    assert value == 0.5


def test_delta_wa_falls_back_to_exponential_when_no_samples():
    estimator = UniversalWAEstimator(latency_samples_per_node=[[]])

    value = estimator.delta_wa(
        delay_ms=10,
        prev_finish_hat_ms=20,
        active_prefix=1,
        delays_ms=[0],
    )

    assert math.isclose(value, math.exp(-10 / 20), rel_tol=1e-9)
