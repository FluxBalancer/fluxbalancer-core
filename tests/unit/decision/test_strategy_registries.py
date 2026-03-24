import pytest

from modules.decision.adapters.outbound.registries.balancer_strategy_registry import (
    BalancerStrategyRegistry,
)
from modules.decision.adapters.outbound.registries.weight_strategy_registry import (
    WeightsStrategyRegistry,
)
from modules.decision.adapters.outbound.strategies.airm_strategy import AIRMStrategy
from modules.decision.adapters.outbound.strategies.electre_strategy import (
    ELECTREStrategy,
)
from modules.decision.adapters.outbound.strategies.lc_strategy import (
    LinearScalarizationStrategy,
)
from modules.decision.adapters.outbound.strategies.saw_strategy import SAWStrategy
from modules.decision.adapters.outbound.strategies.topsis_strategy import (
    TopsisStrategy,
)
from modules.decision.adapters.outbound.weights.entropy_weights_strategy import (
    EntropyWeightsStrategy,
)


@pytest.mark.parametrize(
    "name, expected_type",
    [
        ("topsis", TopsisStrategy),
        (" TOPSIS ", TopsisStrategy),
        ("saw", SAWStrategy),
        ("airm", AIRMStrategy),
        ("electre", ELECTREStrategy),
        ("lc", LinearScalarizationStrategy),
    ],
)
def test_balancer_registry_returns_expected_strategy(name, expected_type):
    registry = BalancerStrategyRegistry()

    strategy = registry.get(name)

    assert isinstance(strategy, expected_type)


def test_balancer_registry_raises_on_unknown_name():
    registry = BalancerStrategyRegistry()

    with pytest.raises(ValueError, match="Неизвестный алгоритм"):
        registry.get("unknown")


def test_weights_registry_returns_entropy():
    registry = WeightsStrategyRegistry()

    strategy = registry.get(" entropy ")

    assert isinstance(strategy, EntropyWeightsStrategy)


def test_weights_registry_raises_on_unknown_name():
    registry = WeightsStrategyRegistry()

    with pytest.raises(ValueError, match="Неизвестный алгоритм весов"):
        registry.get("unknown")


def test_weights_registry_fixed_currently_not_supported():
    registry = WeightsStrategyRegistry()

    with pytest.raises(ValueError, match="Неизвестный алгоритм весов"):
        registry.get("fixed")
