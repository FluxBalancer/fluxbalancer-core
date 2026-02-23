from typing import Protocol

from modules.replication.domain.completion.base import CompletionPolicyInput
from modules.replication.domain.model.execution_result import ExecutionResult
from modules.replication.domain.model.replication_command import ReplicationCommand
from modules.replication.domain.model.replication_plan import ReplicationPlan


class ReplicationRunner(Protocol):
    async def execute(
        self,
        cmd: ReplicationCommand,
        plan: ReplicationPlan,
        policy_input: CompletionPolicyInput,
        deadline_at: float,
    ) -> ExecutionResult: ...
