from dataclasses import dataclass

from .replication_target import ReplicationTarget


@dataclass(slots=True)
class ReplicationPlan:
    """План репликации запроса."""

    targets: list[ReplicationTarget]
