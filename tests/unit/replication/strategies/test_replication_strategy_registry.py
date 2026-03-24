import pytest

from modules.replication.adapters.outbound.registries.replication_strategy_registry import (
    ReplicationStrategyRegistry,
)
from modules.replication.adapters.outbound.strategies.fixed_r_way import (
    FixedParallelReplication,
)
from modules.replication.adapters.outbound.strategies.hedged_requests import (
    HedgedReplication,
)
from modules.replication.adapters.outbound.strategies.speculative_execution import (
    SpeculativeReplication,
)


def test_registry_returns_fixed_by_default():
    registry = ReplicationStrategyRegistry()

    strategy = registry.get(None)

    assert isinstance(strategy, FixedParallelReplication)
    assert strategy.r == 2


def test_registry_returns_fixed_by_name():
    registry = ReplicationStrategyRegistry()

    strategy = registry.get("fixed")

    assert isinstance(strategy, FixedParallelReplication)


def test_registry_returns_hedged_by_name():
    registry = ReplicationStrategyRegistry()

    strategy = registry.get("hedged")

    assert isinstance(strategy, HedgedReplication)
    assert strategy.r_max == 10
    assert strategy.tau_ms == 500


def test_registry_returns_speculative_by_name():
    registry = ReplicationStrategyRegistry()

    strategy = registry.get("speculative")

    assert isinstance(strategy, SpeculativeReplication)
    assert strategy.r_max == 10
    assert strategy.threshold_ms == 120


def test_registry_raises_on_unknown_strategy():
    registry = ReplicationStrategyRegistry()

    with pytest.raises(ValueError, match="Неизвестная стратегия репликации"):
        registry.get("unknown")
