import pytest

from modules.replication.adapters.outbound.registries.completion_strategy_registry import (
    CompletionStrategyRegistry,
)
from modules.replication.domain.completion import (
    FirstValidPolicy,
    KOutOfNPolicy,
    MajorityPolicy,
    QuorumPolicy,
)


def test_registry_returns_first_valid_by_default():
    registry = CompletionStrategyRegistry()

    strategy = registry.get(None)

    assert isinstance(strategy, FirstValidPolicy)


def test_registry_returns_majority_with_required_expected_n():
    registry = CompletionStrategyRegistry()

    strategy = registry.get("majority", n_total=5)

    assert isinstance(strategy, MajorityPolicy)
    assert strategy.expected_n == 5
    assert strategy.required == 3


def test_registry_returns_quorum_using_explicit_k():
    registry = CompletionStrategyRegistry()

    strategy = registry.get("quorum", k=2, n_total=5)

    assert isinstance(strategy, QuorumPolicy)
    assert strategy.quorum_size == 2


def test_registry_returns_quorum_using_default_majority_rule():
    registry = CompletionStrategyRegistry()

    strategy = registry.get("quorum", k=None, n_total=4)

    assert isinstance(strategy, QuorumPolicy)
    assert strategy.quorum_size == 3


def test_registry_returns_k_out_of_n():
    registry = CompletionStrategyRegistry()

    strategy = registry.get("k_out_of_n", k=2)

    assert isinstance(strategy, KOutOfNPolicy)
    assert strategy.k == 2


def test_registry_raises_on_unknown_strategy():
    registry = CompletionStrategyRegistry()

    with pytest.raises(ValueError, match="Неизвестная стратегия завершения"):
        registry.get("unknown")
