import pytest

from modules.replication.domain.services.adaptive_selecctor import (
    adaptive_selector_replicas,
)
from modules.replication.domain.services.work_amplification.universal_wa import (
    UniversalWAEstimator,
)

R_MAX = 3


@pytest.mark.parametrize(
    "lambda_cost, expected",
    [
        (0.5, lambda r: 1 <= r <= R_MAX),
        (0, lambda r: r == R_MAX),
        (float("inf"), lambda r: r == 1),
    ],
)
def test_adaptive_selector_returns_valid_r(lambda_cost, expected):
    samples = [
        [100, 110, 120],
        [90, 95, 105],
        [200, 210, 220],
    ]

    estimator = UniversalWAEstimator(samples)

    r = adaptive_selector_replicas(
        r_max=R_MAX,
        lambda_cost=lambda_cost,
        wa_estimator=estimator,
        delays_ms=[0, 50, 100],
        samples_per_node=samples,
    )

    assert expected(r)
