from unittest.mock import AsyncMock, Mock

import pytest

from modules.gateway.application.dto.brs import BRSRequest
from modules.replication.application.services.replication_planner import (
    PlannerConfig,
    ReplicationPlanner,
)
from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.model.replication_target import ReplicationTarget


def _brs(
    *,
    deadline_ms: int = 1000,
    replication_strategy_name: str | None = None,
    replications_count: int | None = None,
    replicate_all: bool = False,
    replications_adaptive: bool | None = None,
):
    return BRSRequest(
        service=None,
        replications_count=replications_count,
        replicate_all=replicate_all,
        deadline_ms=deadline_ms,
        balancer_strategy_name=None,
        weights_strategy_name=None,
        replication_strategy_name=replication_strategy_name,
        completion_strategy_name=None,
        completion_k=None,
        replications_adaptive=replications_adaptive,
    )


@pytest.mark.asyncio
async def test_planner_fixed_strategy_uses_policy_count_without_latency_sampling():
    chooser = AsyncMock()
    chooser.rank_nodes.return_value = [
        ("n1", "host1", 8001),
        ("n2", "host2", 8002),
        ("n3", "host3", 8003),
    ]

    strategy = AsyncMock()
    strategy.build.return_value = ReplicationPlan(
        targets=[
            ReplicationTarget("n1", "host1", 8001),
            ReplicationTarget("n2", "host2", 8002),
        ]
    )

    registry = Mock()
    registry.get.return_value = strategy

    metrics_repo = AsyncMock()

    planner = ReplicationPlanner(
        chooser=chooser,
        policy=Mock(resolve_count=Mock(return_value=2)),
        strategy_registry=registry,
        metrics_repository=metrics_repo,
        config=PlannerConfig(),
    )

    plan = await planner.build(
        _brs(replication_strategy_name="fixed"),
        request_profile="cpu:1",
    )

    assert len(plan.targets) == 2
    assert plan.r_eff == 2

    strategy.build.assert_awaited_once()
    kwargs = strategy.build.await_args.kwargs
    assert kwargs["max_replicas"] == 2
    assert kwargs["tau_ms"] is None
    assert kwargs["latency_samples_per_node"] is None
    assert kwargs["backup_max_inflight"] is None

    metrics_repo.get_latency_samples.assert_not_called()


@pytest.mark.asyncio
async def test_planner_hedged_disables_replication_when_not_enough_samples():
    chooser = AsyncMock()
    chooser.rank_nodes.return_value = [
        ("n1", "host1", 8001),
        ("n2", "host2", 8002),
        ("n3", "host3", 8003),
    ]

    strategy = AsyncMock()
    strategy.build.return_value = ReplicationPlan(
        targets=[ReplicationTarget("n1", "host1", 8001, delay_ms=0)]
    )

    registry = Mock()
    registry.get.return_value = strategy

    metrics_repo = AsyncMock()
    # profile samples
    metrics_repo.get_latency_samples.side_effect = [
        [100, 110],  # меньше min_samples
        [120, 130],  # global samples тоже меньше min_samples
    ]

    planner = ReplicationPlanner(
        chooser=chooser,
        policy=Mock(resolve_count=Mock(return_value=3)),
        strategy_registry=registry,
        metrics_repository=metrics_repo,
        config=PlannerConfig(min_samples=4),
    )

    plan = await planner.build(
        _brs(replication_strategy_name="hedged"),
        request_profile="cpu:1",
    )

    assert len(plan.targets) == 1
    assert plan.targets[0].node_id == "n1"

    kwargs = strategy.build.await_args.kwargs
    assert kwargs["ranked"] == [("n1", "host1", 8001)]
    assert kwargs["max_replicas"] == 1
    assert kwargs["tau_ms"] is None
    assert kwargs["latency_samples_per_node"] is None
    assert kwargs["backup_max_inflight"] is None


@pytest.mark.asyncio
async def test_planner_hedged_limits_replica_count_by_deadline_and_passes_tau():
    chooser = AsyncMock()
    chooser.rank_nodes.return_value = [
        ("n1", "host1", 8001),
        ("n2", "host2", 8002),
        ("n3", "host3", 8003),
        ("n4", "host4", 8004),
    ]

    strategy = AsyncMock()
    strategy.build.return_value = ReplicationPlan(
        targets=[
            ReplicationTarget("n1", "host1", 8001, delay_ms=0),
            ReplicationTarget("n2", "host2", 8002, delay_ms=40),
            ReplicationTarget("n3", "host3", 8003, delay_ms=80),
        ]
    )

    registry = Mock()
    registry.get.return_value = strategy

    metrics_repo = AsyncMock()
    metrics_repo.get_latency_samples.side_effect = [
        # _estimate_backup_delay_ms(profile on primary)
        [40, 40, 40, 40, 40, 40, 40, 40],
        # latency samples for ranked_cut nodes
        [41, 42, 43],
        [50, 51, 52],
        [60, 61, 62],
    ]

    planner = ReplicationPlanner(
        chooser=chooser,
        policy=Mock(resolve_count=Mock(return_value=4)),
        strategy_registry=registry,
        metrics_repository=metrics_repo,
        config=PlannerConfig(
            min_samples=4,
            max_adaptive_replicas=3,
            hedge_quantile=40,
        ),
    )

    plan = await planner.build(
        _brs(
            deadline_ms=200,
            replication_strategy_name="hedged",
            replications_adaptive=False,
        ),
        request_profile="cpu:1",
    )

    assert len(plan.targets) == 3
    assert plan.r_eff == 3

    call = strategy.build.await_args
    args = call.args
    kwargs = call.kwargs

    assert args[0] == [
        ("n1", "host1", 8001),
        ("n2", "host2", 8002),
        ("n3", "host3", 8003),
    ]
    assert kwargs["tau_ms"] == 40
    assert kwargs["max_replicas"] == 3
    assert kwargs["backup_max_inflight"] == planner.config.backup_max_inflight
    assert kwargs["latency_samples_per_node"] == [
        [41, 42, 43],
        [50, 51, 52],
        [60, 61, 62],
    ]


@pytest.mark.asyncio
async def test_planner_uses_global_samples_when_profile_samples_insufficient():
    chooser = AsyncMock()
    chooser.rank_nodes.return_value = [
        ("n1", "host1", 8001),
        ("n2", "host2", 8002),
    ]

    strategy = AsyncMock()
    strategy.build.return_value = ReplicationPlan(
        targets=[
            ReplicationTarget("n1", "host1", 8001, delay_ms=0),
            ReplicationTarget("n2", "host2", 8002, delay_ms=50),
        ]
    )

    registry = Mock()
    registry.get.return_value = strategy

    metrics_repo = AsyncMock()
    metrics_repo.get_latency_samples.side_effect = [
        [10, 20],  # profile primary -> мало
        [50, 55, 60, 65],  # global primary -> хватает
        [51, 52, 53],  # node1 in ranked_cut
        [70, 71, 72],  # node2 in ranked_cut
    ]

    planner = ReplicationPlanner(
        chooser=chooser,
        policy=Mock(resolve_count=Mock(return_value=2)),
        strategy_registry=registry,
        metrics_repository=metrics_repo,
        config=PlannerConfig(min_samples=4),
    )

    plan = await planner.build(
        _brs(
            deadline_ms=500,
            replication_strategy_name="hedged",
            replications_adaptive=False,
        ),
        request_profile="cpu:1",
    )

    assert len(plan.targets) == 2
    kwargs = strategy.build.await_args.kwargs
    assert (
        kwargs["tau_ms"] == 56
    )  # percentile 40 on [50,55,60,65] -> 56.0 -> round -> 56
