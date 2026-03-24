import pytest
from starlette.requests import Request

from modules.gateway.adapters.inbound.http.brs_parser import BRSParser


def _make_request(headers: dict[str, str]) -> Request:
    raw_headers = [
        (key.lower().encode("latin-1"), value.encode("latin-1"))
        for key, value in headers.items()
    ]
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/cpu",
        "headers": raw_headers,
        "query_string": b"",
    }
    return Request(scope)


def test_parse_minimal_valid_request():
    request = _make_request(
        {
            "X-Balancer-Deadline": "1500",
        }
    )

    brs = BRSParser.parse(request)

    assert brs.service == ""
    assert brs.deadline_ms == 1500
    assert brs.replicate_all is False
    assert brs.replications_count is None
    assert brs.replication_strategy_name is None
    assert brs.balancer_strategy_name is None
    assert brs.weights_strategy_name is None
    assert brs.completion_strategy_name is None
    assert brs.completion_k is None
    assert brs.replications_adaptive is False


def test_parse_full_request_with_normalization():
    request = _make_request(
        {
            "X-Service": "video",
            "X-Balancer-Deadline": "2000",
            "X-Replications-All": "true",
            "X-Replications-Count": "3",
            "X-Replications-Strategy": "  HeDgEd ",
            "X-Balancer-Strategy": "  TOPSIS ",
            "X-Weights-Strategy": "  EnTrOpY ",
            "X-Completion-Strategy": "  QuOrUm ",
            "X-Completion-K": "2",
            "X-Replications-Adaptive": "true",
        }
    )

    brs = BRSParser.parse(request)

    assert brs.service == "video"
    assert brs.deadline_ms == 2000
    assert brs.replicate_all is True
    assert brs.replications_count == 3
    assert brs.replication_strategy_name == "hedged"
    assert brs.balancer_strategy_name == "topsis"
    assert brs.weights_strategy_name == "entropy"
    assert brs.completion_strategy_name == "quorum"
    assert brs.completion_k == 2
    assert brs.replications_adaptive is True


def test_parse_replications_count_true_uses_default():
    request = _make_request(
        {
            "X-Balancer-Deadline": "1500",
            "X-Replications-Count": "true",
        }
    )

    brs = BRSParser.parse(request)

    assert brs.replications_count == BRSParser.DEFAULT_REPLICATIONS


@pytest.mark.parametrize(
    "headers, expected_message",
    [
        ({}, "X-Balancer-Deadline"),
        ({"X-Balancer-Deadline": "0"}, "положительным целым числом"),
        ({"X-Balancer-Deadline": "-1"}, "положительным целым числом"),
        ({"X-Balancer-Deadline": "abc"}, "положительным целым числом"),
    ],
)
def test_parse_deadline_validation(headers, expected_message):
    request = _make_request(headers)

    with pytest.raises(ValueError, match=expected_message):
        BRSParser.parse(request)


@pytest.mark.parametrize(
    "value",
    ["0", "-1", "abc", "1.5", ""],
)
def test_parse_replications_count_invalid(value):
    request = _make_request(
        {
            "X-Balancer-Deadline": "1000",
            "X-Replications-Count": value,
        }
    )

    with pytest.raises(ValueError, match="X-Replications-Count"):
        BRSParser.parse(request)


@pytest.mark.parametrize("value", ["yes", "1", "maybe", ""])
def test_parse_replicate_all_invalid(value):
    request = _make_request(
        {
            "X-Balancer-Deadline": "1000",
            "X-Replications-All": value,
        }
    )

    with pytest.raises(ValueError, match="X-Replications-All"):
        BRSParser.parse(request)


@pytest.mark.parametrize(
    "header_name",
    [
        "X-Replications-Strategy",
        "X-Balancer-Strategy",
        "X-Weights-Strategy",
    ],
)
def test_parse_strategy_headers_reject_empty_values(header_name):
    request = _make_request(
        {
            "X-Balancer-Deadline": "1000",
            header_name: "   ",
        }
    )

    with pytest.raises(ValueError):
        BRSParser.parse(request)


def test_parse_replications_adaptive_invalid_value_returns_none():
    request = _make_request(
        {
            "X-Balancer-Deadline": "1000",
            "X-Replications-Adaptive": "not-a-bool",
        }
    )

    brs = BRSParser.parse(request)

    assert brs.replications_adaptive is None


def test_parse_completion_k_invalid_bubbles_up():
    request = _make_request(
        {
            "X-Balancer-Deadline": "1000",
            "X-Completion-K": "abc",
        }
    )

    with pytest.raises(ValueError):
        BRSParser.parse(request)
