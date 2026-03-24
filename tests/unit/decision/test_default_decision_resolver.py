from unittest.mock import Mock

import pytest

from modules.decision.application.services.default_decision_resolver import (
    DefaultDecisionResolver,
)
from modules.gateway.application.dto.brs import BRSRequest


def _brs(
    *,
    balancer_strategy_name=None,
    weights_strategy_name=None,
):
    return BRSRequest(
        service=None,
        replications_count=None,
        replicate_all=False,
        deadline_ms=1000,
        balancer_strategy_name=balancer_strategy_name,
        weights_strategy_name=weights_strategy_name,
        replication_strategy_name=None,
        completion_strategy_name=None,
        completion_k=None,
        replications_adaptive=None,
    )


def test_resolve_balancer_returns_default_when_not_specified():
    balancer_provider = Mock()
    weights_provider = Mock()
    default_balancer = object()
    default_weights = object()

    resolver = DefaultDecisionResolver(
        balancer_provider=balancer_provider,
        weights_provider=weights_provider,
        default_balancer=default_balancer,
        default_weights=default_weights,
    )

    result = resolver.resolve_balancer(_brs())

    assert result is default_balancer
    balancer_provider.get.assert_not_called()


def test_resolve_weights_returns_default_when_not_specified():
    balancer_provider = Mock()
    weights_provider = Mock()
    default_balancer = object()
    default_weights = object()

    resolver = DefaultDecisionResolver(
        balancer_provider=balancer_provider,
        weights_provider=weights_provider,
        default_balancer=default_balancer,
        default_weights=default_weights,
    )

    result = resolver.resolve_weights(_brs())

    assert result is default_weights
    weights_provider.get.assert_not_called()


def test_resolve_balancer_uses_provider_for_explicit_name():
    balancer_provider = Mock()
    weights_provider = Mock()
    explicit = object()
    balancer_provider.get.return_value = explicit

    resolver = DefaultDecisionResolver(
        balancer_provider=balancer_provider,
        weights_provider=weights_provider,
        default_balancer=object(),
        default_weights=object(),
    )

    result = resolver.resolve_balancer(_brs(balancer_strategy_name="topsis"))

    assert result is explicit
    balancer_provider.get.assert_called_once_with("topsis")


def test_resolve_weights_uses_provider_for_explicit_name():
    balancer_provider = Mock()
    weights_provider = Mock()
    explicit = object()
    weights_provider.get.return_value = explicit

    resolver = DefaultDecisionResolver(
        balancer_provider=balancer_provider,
        weights_provider=weights_provider,
        default_balancer=object(),
        default_weights=object(),
    )

    result = resolver.resolve_weights(_brs(weights_strategy_name="entropy"))

    assert result is explicit
    weights_provider.get.assert_called_once_with("entropy")


def test_resolve_balancer_wraps_provider_error():
    balancer_provider = Mock()
    balancer_provider.get.side_effect = ValueError("bad strategy")

    resolver = DefaultDecisionResolver(
        balancer_provider=balancer_provider,
        weights_provider=Mock(),
        default_balancer=object(),
        default_weights=object(),
    )

    with pytest.raises(ValueError, match="неизвестная стратегия балансировки"):
        resolver.resolve_balancer(_brs(balancer_strategy_name="oops"))


def test_resolve_weights_wraps_provider_error():
    weights_provider = Mock()
    weights_provider.get.side_effect = ValueError("bad strategy")

    resolver = DefaultDecisionResolver(
        balancer_provider=Mock(),
        weights_provider=weights_provider,
        default_balancer=object(),
        default_weights=object(),
    )

    with pytest.raises(ValueError, match="неизвестная стратегия весов"):
        resolver.resolve_weights(_brs(weights_strategy_name="oops"))
