import asyncio
import hashlib
import logging
import time
from asyncio import Task

from aiohttp import ClientSession, ClientTimeout

from core.application.ports.strategy_provider import StrategyProvider
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
from src.modules.replication.domain.completion import (
    CompletionPolicy,
    ReplicaReply,
)

logger = logging.getLogger("replication.runner")


class AiohttpReplicationRunner(ReplicationRunner):
    """Выполняет репликации (hedged / fixed / speculative) по плану.

    Отвечает за:
      - запуск HTTP-запросов согласно delay_ms;
      - применение политики завершения (first/quorum/majority/k-out-of-n);
      - отмену незавершённых задач;
      - запись latency победителя в MetricsRepository.

    ВАЖНО:
    - Политика завершения должна быть выбрана менеджером/планировщиком.
    - По умолчанию используется FirstValid (k=1), как в tail-latency mitigation.
    """

    def __init__(
            self,
            client: ClientSession,
            latency_recorder: LatencyRecorder,
            completion_policy_strategy: StrategyProvider[CompletionPolicy],
    ):
        self.client = client
        self.latency_recorder = latency_recorder
        self.completion_policy_strategy = completion_policy_strategy

    async def execute(
            self,
            cmd: ReplicationCommand,
            plan: ReplicationPlan,
            policy_input: CompletionPolicyInput,
            deadline_at: float,
    ) -> ExecutionResult:
        """Выполняет план репликации.

        Args:
            cmd: Данные команды репликации
            plan: План репликации
            policy_input:
            deadline_at:

        Returns:
            Ответ, выбранный политикой завершения.

        Raises:
            RuntimeError: Если ни одна реплика не дала валидный результат.
        """
        completion_strategy: CompletionPolicy = self.completion_policy_strategy.get(
            name=policy_input.strategy_name, k=policy_input.k
        )

        logger.info(f"Replication targets: {plan.targets}")

        tasks: list[Task[ReplicaReply]] = [
            asyncio.create_task(
                self._call_one(target=t, cmd=cmd, deadline_at=deadline_at)
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
            pending: set[Task[ReplicaReply]] = set(tasks)
            done: set[Task[ReplicaReply]]
            while pending:
                remaining = deadline_at - time.perf_counter()
                if remaining <= 0:
                    break

                done, pending = await asyncio.wait(
                    pending, timeout=remaining, return_when=asyncio.FIRST_COMPLETED
                )

                if not done:
                    break

                for task in done:
                    reply: ReplicaReply = await task
                    completion_strategy.push(reply)

                    if completion_strategy.is_done():
                        winner: ReplicaReply = completion_strategy.choose()

                        for p in pending:
                            p.cancel()

                        await asyncio.gather(*pending, return_exceptions=True)
                        await self.latency_recorder.record(
                            node_id=winner.node_id, latency_ms=winner.latency_ms
                        )

                        return ExecutionResult(
                            node_id=winner.node_id,
                            status=winner.status,
                            body=winner.raw_body,
                            headers={
                                **headers,
                                "X-Winner-Socket": winner.node_id,
                            },
                            latency_ms=winner.latency_ms,
                        )

            logger.error(
                f"Replication failed. "
                f"Replies collected: {completion_strategy.replies}"
            )
            if completion_strategy.replies:
                best = min(
                    completion_strategy.replies,
                    key=lambda r: r.latency_ms
                )

                return ExecutionResult(
                    node_id=best.node_id,
                    status=best.status if best.status else 504,
                    body=best.raw_body or b"",
                    headers={
                        **headers,
                        "X-Winner-Socket": best.node_id,
                        "X-Replication-Error": "degraded",
                    },
                    latency_ms=best.latency_ms,
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
            )
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _call_one(
            self, *, target: ReplicationTarget, cmd: ReplicationCommand, deadline_at: float
    ) -> ReplicaReply:
        """Делает один HTTP вызов к реплике.

        Args:
            cmd: Данные команды репликации
            target: Цель вызова (узел/порт/задержка).

        Returns:
            ReplicaReply.
        """
        if target.delay_ms > 0:
            await asyncio.sleep(target.delay_ms / 1000.0)

        remaining = deadline_at - time.perf_counter()
        if remaining <= 0:
            return _get_empty_replica_reply(node_id=target.node_id, latency_ms=0.0)

        logger.info(f"Create replication on {target.host}:{target.port}")

        url: str = f"http://{target.host}:{target.port}{cmd.path}"
        timeout = ClientTimeout(total=remaining)

        t0: float = time.perf_counter()
        try:
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
                    ok=ok,
                    value=value,
                    raw_body=raw or b"",
                    status=int(resp.status),
                    latency_ms=latency_ms,
                )
        except asyncio.TimeoutError:
            return _get_empty_replica_reply(
                node_id=target.node_id, latency_ms=(time.perf_counter() - t0) * 1000.0
            )
        except asyncio.CancelledError:
            logger.debug(f"Replication cancelled for {target.node_id}")
            raise
        except Exception:
            return _get_empty_replica_reply(
                node_id=target.node_id,
                latency_ms=(time.perf_counter() - t0) * 1000.0,
                status=599,
            )


def _get_empty_replica_reply(
        node_id: str, latency_ms: float, status: int = 598
) -> ReplicaReply:
    return ReplicaReply(
        node_id=node_id,
        ok=False,
        value="",
        raw_body=b"",
        status=status,
        latency_ms=latency_ms,
    )
