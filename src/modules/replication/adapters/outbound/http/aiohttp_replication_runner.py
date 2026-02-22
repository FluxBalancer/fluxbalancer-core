import asyncio
import hashlib
import time
from asyncio import Task
from typing import Callable

from aiohttp import ClientSession

from modules.replication.application.ports.outbound.latency_recorder import (
    LatencyRecorder,
)
from modules.replication.application.ports.outbound.replication_runnter import (
    ReplicationRunner,
)
from modules.replication.domain.model.execution_result import ExecutionResult
from modules.replication.domain.model.replication_command import ReplicationCommand
from modules.replication.domain.model.replication_plan import ReplicationPlan
from modules.replication.domain.model.replication_target import ReplicationTarget
from src.modules.replication.domain.completion import (
    CompletionPolicy,
    FirstValidPolicy,
    ReplicaReply,
)


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
        *,
        completion_factory: Callable[[], CompletionPolicy] | None = None,
    ):
        self.client = client
        self.latency_recorder = latency_recorder
        # TODO: add completion strategy
        self.completion_factory = completion_factory or (lambda: FirstValidPolicy())

    async def execute(
        self, cmd: ReplicationCommand, plan: ReplicationPlan
    ) -> ExecutionResult:
        """Выполняет план репликации.

        Args:
            cmd: Данные команды репликации
            plan: План репликации.

        Returns:
            Ответ, выбранный политикой завершения.

        Raises:
            RuntimeError: Если ни одна реплика не дала валидный результат.
        """
        policy: CompletionPolicy = self.completion_factory()
        tasks: list[Task[ReplicaReply]] = [
            asyncio.create_task(
                self._call_one(
                    target=t,
                    cmd=cmd,
                )
            )
            for t in plan.targets
        ]

        try:
            pending: set[Task[ReplicaReply]] = set(tasks)
            done: set[Task[ReplicaReply]]

            while pending:
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    reply: ReplicaReply = await task
                    policy.push(reply)

                    if policy.is_done():
                        winner = policy.choose()

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
                            headers={},
                            latency_ms=winner.latency_ms,
                        )

            raise RuntimeError(
                "replication: нет ответа, удовлетворяющего политике завершения"
            )
        finally:
            for t in tasks:
                if not t.done():
                    t.cancel()

    async def _call_one(
        self, *, target: ReplicationTarget, cmd: ReplicationCommand
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

        url: str = f"http://{target.host}:{target.port}{cmd.path}"

        t0: float = time.perf_counter()
        try:
            async with self.client.request(
                cmd.method,
                url,
                params=dict(cmd.query),
                headers=dict(cmd.headers),
                data=cmd.body,
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

        except asyncio.CancelledError:
            raise
        except Exception:
            latency_ms = (time.perf_counter() - t0) * 1000.0

            return ReplicaReply(
                node_id=target.node_id,
                ok=False,
                value="",
                raw_body=b"",
                status=599,
                latency_ms=latency_ms,
            )
