from dataclasses import dataclass
from typing import Mapping


@dataclass(slots=True)
class ReplicationCommand:
    method: str
    path: str
    query: Mapping[str, str]
    headers: Mapping[str, str]
    body: bytes
