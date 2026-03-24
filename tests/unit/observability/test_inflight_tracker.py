import asyncio

import pytest

from modules.observability.application.services.inflight_tracker import (
    InflightTracker,
)


@pytest.mark.asyncio
async def test_inflight_tracker_tracks_context_manager_lifecycle():
    tracker = InflightTracker()

    assert await tracker.get("node-1") == 0

    async with tracker.track("node-1"):
        assert await tracker.get("node-1") == 1
        assert await tracker.is_greater_than_limit("node-1", 0) is True
        assert await tracker.is_greater_than_limit("node-1", 1) is False

    assert await tracker.get("node-1") == 0


@pytest.mark.asyncio
async def test_inflight_tracker_counts_parallel_requests():
    tracker = InflightTracker()
    started = asyncio.Event()
    release = asyncio.Event()

    async def worker():
        async with tracker.track("node-1"):
            started.set()
            await release.wait()

    task = asyncio.create_task(worker())
    await started.wait()

    async with tracker.track("node-1"):
        assert await tracker.get("node-1") == 2
        assert await tracker.is_greater_than_limit("node-1", 1) is True

    release.set()
    await task

    assert await tracker.get("node-1") == 0
