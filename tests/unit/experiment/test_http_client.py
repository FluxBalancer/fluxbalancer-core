import pytest
from unittest.mock import AsyncMock, patch

from src.experiment_runner.client import HTTPClient


@pytest.mark.asyncio
async def test_http_client_success():
    client = HTTPClient("http://test", {}, 10)

    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.read.return_value = b'{"ok": true}'
    mock_resp.headers = {}

    session = AsyncMock()
    session.get.return_value.__aenter__.return_value = mock_resp

    client.session = session

    result = await client.request("1", "endpoint")

    assert result.ok
    assert result.status == 200