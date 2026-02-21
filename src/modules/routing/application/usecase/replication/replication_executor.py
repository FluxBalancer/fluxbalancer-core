import asyncio
import hashlib
import time
from typing import Callable

from aiohttp import ClientSession
from starlette.requests import Request
from starlette.responses import Response

from src.modules.replication.domain.replication_plan import ReplicationPlan
from src.modules.replication.domain.replication_target import ReplicationTarget
from src.modules.routing.application.ports.outbound.metrics.metrics_repository import (
    MetricsRepository,
)
from src.modules.routing.domain.policies.completion import (
    CompletionPolicy,
    FirstValidPolicy,
    ReplicaReply,
)


class ReplicationExecutor:
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
        metrics_repo: MetricsRepository,
        *,
        completion_factory: Callable[[], CompletionPolicy] | None = None,
    ):
        self.metrics_repo = metrics_repo
        self.completion_factory = completion_factory or (lambda: FirstValidPolicy())

    async def execute(
        self,
        request: Request,
        plan: ReplicationPlan,
        client: ClientSession,
    ) -> Response:
        """Выполняет план репликации.

        Args:
            request: Входящий HTTP-запрос.
            plan: План репликации.
            client: aiohttp клиент.

        Returns:
            Ответ, выбранный политикой завершения.

        Raises:
            RuntimeError: Если ни одна реплика не дала валидный результат.
        """
        body_bytes = await request.body()
        headers = dict(request.headers)
        method = request.method
        path = request.url.path
        params = request.query_params

        policy = self.completion_factory()

        tasks = [
            asyncio.create_task(
                self._call_one(
                    target=t,
                    method=method,
                    path=path,
                    params=params,
                    headers=headers,
                    body=body_bytes,
                    client=client,
                )
            )
            for t in plan.targets
        ]

        try:
            pending = set(tasks)
            while pending:
                done, pending = await asyncio.wait(
                    pending,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                for t in done:
                    reply = await t
                    policy.push(reply)

                    if policy.is_done():
                        winner = policy.choose()
                        # отменяем остальных
                        for p in pending:
                            p.cancel()
                        await asyncio.gather(*pending, return_exceptions=True)

                        await self.metrics_repo.add_latency(
                            winner.node_id, winner.latency_ms
                        )

                        return Response(
                            content=winner.raw_body,
                            status_code=winner.status,
                            headers={},  # можно прокинуть часть заголовков при необходимости
                            media_type="application/json",
                        )

            raise RuntimeError(
                "replication: нет ответа, удовлетворяющего политике завершения"
            )

        finally:
            # на всякий случай подчистим
            for t in tasks:
                if not t.done():
                    t.cancel()

    async def _call_one(
        self,
        *,
        target: ReplicationTarget,
        method: str,
        path: str,
        params,
        headers: dict,
        body: bytes,
        client: ClientSession,
    ) -> ReplicaReply:
        """Делает один HTTP вызов к реплике.

        Args:
            target: Цель вызова (узел/порт/задержка).
            method: HTTP метод.
            path: Путь.
            params: Query params.
            headers: Заголовки.
            body: Тело запроса.
            client: aiohttp сессия.

        Returns:
            ReplicaReply.
        """
        if target.delay_ms > 0:
            await asyncio.sleep(target.delay_ms / 1000.0)

        url = f"http://{target.host}:{target.port}{path}"

        t0 = time.perf_counter()
        try:
            async with client.request(
                method,
                url,
                params=params,
                headers=headers,
                data=body,
            ) as resp:
                raw = await resp.read()
                latency_ms = (time.perf_counter() - t0) * 1000.0

                # валидатор: 2xx и непустое тело
                ok = 200 <= resp.status < 300 and raw is not None

                # value: хэш тела (для quorum/majority)
                value = hashlib.sha256(raw or b"").hexdigest()

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
