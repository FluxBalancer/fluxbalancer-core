from unittest.mock import AsyncMock

import pytest
from starlette.requests import Request
from starlette.responses import Response

from modules.gateway.application.dto.brs import BRSRequest
from modules.replication.application.services.replication_manager import (
    ReplicationManager,
)
from modules.replication.domain.model.execution_result import ExecutionResult
from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.model.replication_target import ReplicationTarget


def _make_request(
    *,
    method: str = "GET",
    path: str = "/cpu",
    query_string: str = "seconds=1",
    headers: dict[str, str] | None = None,
    body: bytes = b"",
) -> Request:
    raw_headers = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in (headers or {}).items()
    ]
    scope = {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": query_string.encode(),
        "headers": raw_headers,
    }

    received = False

    async def receive():
        nonlocal received
        if received:
            return {
                "type": "http.request",
                "body": b"",
                "more_body": False,
            }

        received = True
        return {
            "type": "http.request",
            "body": body,
            "more_body": False,
        }

    return Request(scope, receive=receive)


def _brs(deadline_ms: int = 1000) -> BRSRequest:
    return BRSRequest(
        service=None,
        replications_count=2,
        replicate_all=False,
        deadline_ms=deadline_ms,
        balancer_strategy_name=None,
        weights_strategy_name=None,
        replication_strategy_name="fixed",
        completion_strategy_name="first",
        completion_k=None,
        replications_adaptive=False,
    )


@pytest.mark.asyncio
async def test_manager_returns_false_when_plan_has_only_one_target():
    planner = AsyncMock()
    planner.build.return_value = ReplicationPlan(
        targets=[ReplicationTarget("n1", "host1", 8001)]
    )

    runner = AsyncMock()
    manager = ReplicationManager(planner=planner, executor=runner)

    response, sockets, used = await manager.execute(
        request=_make_request(),
        brs=_brs(),
        request_profile="cpu:1",
    )

    assert used is False
    assert sockets == ""
    assert response.status_code == 300
    assert response.body == b""
    runner.execute.assert_not_called()


@pytest.mark.asyncio
async def test_manager_builds_command_and_wraps_runner_result():
    planner = AsyncMock()
    planner.build.return_value = ReplicationPlan(
        targets=[
            ReplicationTarget("n1", "host1", 8001),
            ReplicationTarget("n2", "host2", 8002),
        ]
    )

    runner = AsyncMock()
    runner.execute.return_value = ExecutionResult(
        node_id="n2",
        status=200,
        body=b'{"ok": true}',
        headers={"X-Test": "1"},
        latency_ms=42.0,
        started_nodes=["host1:8001", "host2:8002"],
    )

    manager = ReplicationManager(planner=planner, executor=runner)

    request = _make_request(
        method="POST",
        path="/mem",
        query_string="seconds=2&mb=256",
        headers={"X-Any": "1"},
        body=b"payload",
    )

    response, sockets, used = await manager.execute(
        request=request,
        brs=_brs(deadline_ms=1500),
        request_profile="mem:2:mid",
    )

    assert used is True
    assert sockets == "host1:8001, host2:8002"
    assert isinstance(response, Response)
    assert response.status_code == 200
    assert response.body == b'{"ok": true}'
    assert response.headers["x-test"] == "1"

    runner.execute.assert_awaited_once()
    call_kwargs = runner.execute.await_args.kwargs

    cmd = call_kwargs["cmd"]
    assert cmd.method == "POST"
    assert cmd.path == "/mem"
    assert dict(cmd.query)["seconds"] == "2"
    assert dict(cmd.query)["mb"] == "256"
    assert cmd.body == b"payload"
    assert cmd.profile == "mem:2:mid"

    policy_input = call_kwargs["policy_input"]
    assert policy_input.strategy_name == "first"
    assert policy_input.k is None
