import asyncio
import hashlib
import logging
import time
from asyncio import Task

from aiohttp import ClientSession, ClientTimeout

from core.application.ports.strategy_provider import StrategyProvider
from modules.observability.application.services.inflight_tracker import InflightTracker
from modules.replication.adapters.outbound.registries.completion_strategy_registry import (
    CompletionAlgorithmName,
)
from modules.replication.application.ports.outbound.latency_recorder import (
    LatencyRecorder,
)
from modules.replication.application.ports.outbound.replication_runner import (
    ReplicationRunner,
)
from modules.replication.domain.completion.base import CompletionPolicyInput
from modules.replication.domain.model.execution_result import ExecutionResult
from modules.replication.domain.model.replication_command import ReplicationCommand
from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.model.replication_target import ReplicationTarget
from src.modules.replication.domain.completion import CompletionPolicy, ReplicaReply

logger = logging.getLogger("replication.runner")


class AiohttpReplicationRunner(ReplicationRunner):
    """
    Строгий replication runner.

    Ключевое отличие:
    delayed backup-реплика стартует только если target.require_idle=True
    и соответствующая нода в этот момент idle.
    """

    def __init__(
        self,
        client: ClientSession,
        latency_recorder: LatencyRecorder,
        completion_policy_strategy: StrategyProvider[CompletionPolicy],
        inflight_tracker: InflightTracker,
    ):
        self.client = client
        self.latency_recorder = latency_recorder
        self.completion_policy_strategy = completion_policy_strategy
        self.inflight_tracker = inflight_tracker

    async def execute(
        self,
        cmd: ReplicationCommand,
        plan: ReplicationPlan,
        policy_input: CompletionPolicyInput,
        deadline_at: float,
    ) -> ExecutionResult:
        completion_strategy: CompletionPolicy = self.completion_policy_strategy.get(
            name=policy_input.strategy_name,
            k=policy_input.k,
            n_total=len(plan.targets),
        )

        logger.info("Replication targets: %s", plan.targets)

        observed_replies: list[ReplicaReply] = []
        started_nodes: list[str] = []

        tasks: list[Task[ReplicaReply | None]] = [
            asyncio.create_task(
                self._call_one(
                    target=t,
                    cmd=cmd,
                    deadline_at=deadline_at,
                    started_nodes=started_nodes,
                )
            )
            for t in plan.targets
        ]

        headers = {
            "X-Replica-Count": str(len(plan.targets)),
            "X-Replica-Effective": str(plan.r_eff or len(plan.targets)),
            "X-Completion-Strategy": policy_input.strategy_name
            or CompletionAlgorithmName.FIRST.value,
        }

        try:
            pending: set[Task[ReplicaReply | None]] = set(tasks)

            while pending:
                remaining = deadline_at - time.perf_counter()
                if remaining <= 0:
                    break

                done, pending = await asyncio.wait(
                    pending,
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if not done:
                    break

                for task in done:
                    reply = await task
                    if reply is None:
                        continue

                    observed_replies.append(reply)
                    completion_strategy.push(reply)

                    if completion_strategy.is_done():
                        winner: ReplicaReply = completion_strategy.choose()

                        for pending_task in pending:
                            pending_task.cancel()

                        if pending:
                            await asyncio.gather(*pending, return_exceptions=True)

                        await self._record_latencies(
                            observed_replies,
                            profile=cmd.profile,
                        )

                        return ExecutionResult(
                            node_id=winner.node_id,
                            status=winner.status,
                            body=winner.raw_body,
                            headers={
                                **headers,
                                "X-Winner-Socket": winner.socket,
                            },
                            latency_ms=winner.latency_ms,
                            started_nodes=started_nodes,
                        )

            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

            logger.error(
                "Replication failed. Replies collected: %s",
                completion_strategy.replies,
            )

            await self._record_latencies(
                observed_replies,
                profile=cmd.profile,
            )

            winner = (
                pick_best(observed_replies, lambda r: r.ok)
                or pick_best(observed_replies, lambda r: 400 <= r.status < 500)
                or pick_best(observed_replies, lambda r: 500 <= r.status < 600)
                or (
                    min(observed_replies, key=lambda r: r.latency_ms)
                    if observed_replies
                    else None
                )
            )

            logger.warning("FALLBACK used, replies=%s", len(observed_replies))

            if winner:
                if winner.ok:
                    error_kind = "degraded_valid_fallback"
                    status = winner.status
                    body = winner.raw_body
                elif 100 <= winner.status <= 599 and winner.status not in {598, 599}:
                    error_kind = "degraded_error_fallback"
                    status = winner.status
                    body = winner.raw_body or b""
                else:
                    error_kind = "deadline_exceeded"
                    status = 504
                    body = b""

                return ExecutionResult(
                    node_id=winner.node_id,
                    status=status,
                    body=body,
                    headers={
                        **headers,
                        "X-Winner-Socket": winner.socket,
                        "X-Replication-Error": error_kind,
                    },
                    latency_ms=winner.latency_ms,
                    started_nodes=started_nodes,
                )

            return ExecutionResult(
                node_id="none",
                status=504,
                body=b"",
                headers={
                    **headers,
                    "X-Winner-Socket": "",
                    "X-Replication-Error": "no_valid_reply",
                },
                latency_ms=0.0,
                started_nodes=started_nodes,
            )
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _record_latencies(
        self,
        replies: list[ReplicaReply],
        profile: str | None = None,
    ) -> None:
        """
        Записываем РЕАЛЬНЫЕ latency, без искусственных множителей.
        """
        for reply in replies:
            try:
                await self.latency_recorder.record(
                    node_id=reply.node_id,
                    latency_ms=reply.latency_ms,
                    profile=profile,
                )
            except Exception:
                logger.exception("failed to record latency for node=%s", reply.node_id)

    async def _call_one(
        self,
        *,
        target: ReplicationTarget,
        cmd: ReplicationCommand,
        deadline_at: float,
        started_nodes: list[str],
    ) -> ReplicaReply | None:
        if target.delay_ms > 0:
            await asyncio.sleep(target.delay_ms / 1000.0)

        remaining = deadline_at - time.perf_counter()
        socket = f"{target.host}:{target.port}"

        if remaining <= 0:
            return _get_empty_replica_reply(
                node_id=target.node_id,
                socket=socket,
                latency_ms=0.0,
            )

        if (
            target.max_inflight is not None
            and await self.inflight_tracker.is_greater_than_limit(
                target.node_id, limit=target.max_inflight
            )
        ):
            logger.info(
                "Skip backup replica to busy node=%s socket=%s",
                target.node_id,
                socket,
            )
            return None

        started_nodes.append(socket)
        logger.info("Create replication on %s", socket)

        url: str = f"http://{target.host}:{target.port}{cmd.path}"
        timeout = ClientTimeout(total=max(0.001, remaining))

        t0: float = time.perf_counter()
        try:
            async with self.inflight_tracker.track(target.node_id):
                async with self.client.request(
                    method=cmd.method,
                    url=url,
                    params=dict(cmd.query),
                    headers=dict(cmd.headers),
                    data=cmd.body,
                    timeout=timeout,
                ) as resp:
                    raw: bytes = await resp.read()
                    latency_ms: float = (time.perf_counter() - t0) * 1000.0

                    ok: bool = 200 <= resp.status < 300 and raw is not None
                    value: str = hashlib.sha256(raw or b"").hexdigest()

                    return ReplicaReply(
                        node_id=target.node_id,
                        socket=socket,
                        ok=ok,
                        value=value,
                        raw_body=raw or b"",
                        status=int(resp.status),
                        latency_ms=latency_ms,
                    )
        except asyncio.TimeoutError:
            return _get_empty_replica_reply(
                node_id=target.node_id,
                socket=socket,
                latency_ms=(time.perf_counter() - t0) * 1000.0,
                status=598,
            )
        except asyncio.CancelledError:
            logger.debug("Replication cancelled for %s", target.node_id)
            raise
        except Exception:
            logger.exception("Replication failed for %s", target.node_id)
            return _get_empty_replica_reply(
                node_id=target.node_id,
                socket=socket,
                latency_ms=(time.perf_counter() - t0) * 1000.0,
                status=599,
            )


def _get_empty_replica_reply(
    node_id: str,
    socket: str,
    latency_ms: float,
    status: int = 598,
) -> ReplicaReply:
    return ReplicaReply(
        node_id=node_id,
        ok=False,
        value="",
        raw_body=b"",
        status=status,
        latency_ms=latency_ms,
        socket=socket,
    )


def pick_best(replies: list[ReplicaReply], predicate):
    candidates = [r for r in replies if predicate(r)]
    return min(candidates, key=lambda r: r.latency_ms) if candidates else None
