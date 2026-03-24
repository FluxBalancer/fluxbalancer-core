import asyncio
from unittest.mock import AsyncMock, Mock

import pytest
from starlette.requests import Request

from modules.gateway.application.dto.brs import BRSRequest
from modules.gateway.application.use_cases.proxy_request import (
    ProxyRequestUseCase,
)


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


def _brs(
    *,
    deadline_ms: int = 1000,
    replications_count: int | None = None,
    replicate_all: bool = False,
    replication_strategy_name: str | None = None,
) -> BRSRequest:
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
        replications_adaptive=False,
    )


class _TrackContext:
    def __init__(self, entered: list[str], node_id: str):
        self.entered = entered
        self.node_id = node_id

    async def __aenter__(self):
        self.entered.append(self.node_id)

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeResponse:
    def __init__(
        self,
        *,
        status: int = 200,
        body: bytes = b"ok",
        headers: dict[str, str] | None = None,
    ):
        self.status = status
        self._body = body
        self.headers = headers or {}

    async def read(self) -> bytes:
        return self._body

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


@pytest.mark.asyncio
async def test_execute_uses_replication_result_and_skips_choose_node():
    choose_node = AsyncMock()
    replication_manager = AsyncMock()
    metrics_repo = AsyncMock()
    client = Mock()
    inflight_tracker = Mock()

    replication_manager.execute.return_value = (
        Mock(body=b"replicated", status_code=201, headers={"X-Replicated": "1"}),
        "host1:8001, host2:8002",
        True,
    )

    use_case = ProxyRequestUseCase(
        choose_node=choose_node,
        replication_manager=replication_manager,
        metrics_repo=metrics_repo,
        client=client,
        inflight_tracker=inflight_tracker,
    )

    request = _make_request(
        headers={"X-Balancer-Deadline": "1000", "X-Replications-Count": "2"}
    )

    result = await use_case.execute(request)

    assert result.body == b"replicated"
    assert result.status == 201
    assert result.headers["X-Replicated"] == "1"
    assert result.socket == "host1:8001, host2:8002"

    choose_node.execute.assert_not_called()
    client.request.assert_not_called()
    metrics_repo.add_latency.assert_not_called()


@pytest.mark.asyncio
async def test_execute_falls_back_to_single_node_when_replication_not_used():
    choose_node = AsyncMock()
    choose_node.execute.return_value = ("node-1", "127.0.0.1", 8080)

    replication_manager = AsyncMock()
    replication_manager.execute.return_value = (
        Mock(body=b"", status_code=300, headers={}),
        "",
        False,
    )

    metrics_repo = AsyncMock()
    entered = []
    inflight_tracker = Mock()
    inflight_tracker.track.side_effect = lambda node_id: _TrackContext(entered, node_id)

    client = Mock()
    client.request.return_value = _FakeResponse(
        status=200,
        body=b'{"ok": true}',
        headers={"X-Upstream": "yes"},
    )

    use_case = ProxyRequestUseCase(
        choose_node=choose_node,
        replication_manager=replication_manager,
        metrics_repo=metrics_repo,
        client=client,
        inflight_tracker=inflight_tracker,
    )

    request = _make_request(
        method="POST",
        path="/mem",
        query_string="seconds=2&mb=256",
        headers={
            "X-Balancer-Deadline": "1500",
            "X-Replications-Count": "2",
            "X-Any": "1",
        },
        body=b"payload",
    )

    result = await use_case.execute(request)

    assert result.status == 200
    assert result.body == b'{"ok": true}'
    assert result.socket == "127.0.0.1:8080"
    assert result.headers["X-Upstream"] == "yes"

    choose_node.execute.assert_awaited_once()
    assert entered == ["node-1"]

    client.request.assert_called_once()
    _, kwargs = client.request.call_args
    assert kwargs["params"]["seconds"] == "2"
    assert kwargs["params"]["mb"] == "256"
    assert kwargs["data"] == b"payload"
    assert kwargs["headers"]["x-any"] == "1"

    metrics_repo.add_latency.assert_awaited_once()
    latency_call = metrics_repo.add_latency.await_args.kwargs
    assert latency_call["node_id"] == "node-1"
    assert latency_call["profile"] == "mem:2:mid"


@pytest.mark.asyncio
async def test_execute_single_node_success_without_replication_headers():
    choose_node = AsyncMock()
    choose_node.execute.return_value = ("node-1", "service.local", 9000)

    replication_manager = AsyncMock()
    metrics_repo = AsyncMock()

    entered = []
    inflight_tracker = Mock()
    inflight_tracker.track.side_effect = lambda node_id: _TrackContext(entered, node_id)

    client = Mock()
    client.request.return_value = _FakeResponse(
        status=204,
        body=b"",
        headers={"X-Test": "ok"},
    )

    use_case = ProxyRequestUseCase(
        choose_node=choose_node,
        replication_manager=replication_manager,
        metrics_repo=metrics_repo,
        client=client,
        inflight_tracker=inflight_tracker,
    )

    request = _make_request(
        headers={"X-Balancer-Deadline": "1200"},
    )

    result = await use_case.execute(request)

    assert result.status == 204
    assert result.body == b""
    assert result.socket == "service.local:9000"
    assert result.headers["X-Test"] == "ok"
    assert entered == ["node-1"]

    replication_manager.execute.assert_not_called()
    metrics_repo.add_latency.assert_awaited_once()


@pytest.mark.asyncio
async def test_execute_returns_504_on_timeout_and_records_latency():
    choose_node = AsyncMock()
    choose_node.execute.return_value = ("node-1", "127.0.0.1", 8080)

    replication_manager = AsyncMock()
    metrics_repo = AsyncMock()

    entered = []
    inflight_tracker = Mock()
    inflight_tracker.track.side_effect = lambda node_id: _TrackContext(entered, node_id)

    class _TimeoutResponse:
        async def __aenter__(self):
            raise asyncio.TimeoutError

        async def __aexit__(self, exc_type, exc, tb):
            return False

    client = Mock()
    client.request.return_value = _TimeoutResponse()

    use_case = ProxyRequestUseCase(
        choose_node=choose_node,
        replication_manager=replication_manager,
        metrics_repo=metrics_repo,
        client=client,
        inflight_tracker=inflight_tracker,
    )

    request = _make_request(headers={"X-Balancer-Deadline": "1000"})

    result = await use_case.execute(request)

    assert result.status == 504
    assert result.body == b""
    assert result.socket == "127.0.0.1:8080"
    assert result.headers["X-Balancer-Error"] == "deadline_exceeded"

    assert entered == ["node-1"]
    metrics_repo.add_latency.assert_awaited_once()


@pytest.mark.asyncio
async def test_execute_returns_500_on_unexpected_error_and_records_latency():
    choose_node = AsyncMock()
    choose_node.execute.return_value = ("node-1", "127.0.0.1", 8080)

    replication_manager = AsyncMock()
    metrics_repo = AsyncMock()

    entered = []
    inflight_tracker = Mock()
    inflight_tracker.track.side_effect = lambda node_id: _TrackContext(entered, node_id)

    class _BrokenResponse:
        async def __aenter__(self):
            raise RuntimeError("boom")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    client = Mock()
    client.request.return_value = _BrokenResponse()

    use_case = ProxyRequestUseCase(
        choose_node=choose_node,
        replication_manager=replication_manager,
        metrics_repo=metrics_repo,
        client=client,
        inflight_tracker=inflight_tracker,
    )

    request = _make_request(headers={"X-Balancer-Deadline": "1000"})

    result = await use_case.execute(request)

    assert result.status == 500
    assert result.body == b""
    assert result.socket == "127.0.0.1:8080"
    assert result.headers["X-Balancer-Error"] == "degraded_error_fallback"

    assert entered == ["node-1"]
    metrics_repo.add_latency.assert_awaited_once()
