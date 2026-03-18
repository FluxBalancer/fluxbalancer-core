from enum import StrEnum
from typing import Callable

from core.application.ports.strategy_provider import StrategyProvider
from modules.replication.domain.completion import (
    CompletionPolicy,
    FirstValidPolicy,
    MajorityPolicy,
    QuorumPolicy,
    KOutOfNPolicy,
)


class CompletionAlgorithmName(StrEnum):
    FIRST = "first"
    MAJORITY = "majority"
    QUORUM = "quorum"
    K_OUT_OF_N = "k_out_of_n"


class CompletionStrategyRegistry(StrategyProvider[CompletionPolicy]):
    """
    Реестр стратегий завершения репликации.
    """

    def __init__(self):
        self._map: dict[str, Callable[..., CompletionPolicy]] = {
            CompletionAlgorithmName.FIRST: lambda **_: FirstValidPolicy(),
            CompletionAlgorithmName.MAJORITY: (
                lambda n_total=1, **_: MajorityPolicy(expected_n=max(1, int(n_total)))
            ),
            CompletionAlgorithmName.QUORUM: (
                lambda k=None, n_total=1, **_: QuorumPolicy(
                    quorum_size=(
                        int(k) if k is not None else max(1, (int(n_total) // 2) + 1)
                    )
                )
            ),
            CompletionAlgorithmName.K_OUT_OF_N: (
                lambda k=1, **_: KOutOfNPolicy(k=max(1, int(k)))
            ),
        }

    def get(self, name: str | None, **kwargs) -> CompletionPolicy:
        if name is None:
            return FirstValidPolicy()

        try:
            key = CompletionAlgorithmName(name.strip().lower())
            factory = self._map[key]

            return factory(**kwargs)

        except Exception as e:
            raise ValueError(f"Неизвестная стратегия завершения: {name}") from e
