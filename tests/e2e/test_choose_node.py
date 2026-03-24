from dataclasses import dataclass
from time import sleep

from fastapi.testclient import TestClient
from httpx import Response

from main import create_app


@dataclass
class ServerCPUResponse:
    cpu_burn: bool
    seconds: float
    complexity: int
    port: int
    cpu_util: float
    mem_util: float
    net_in_bytes: int
    net_out_bytes: int


def test_choose_node_e2e():
    app = create_app()

    seconds = 1

    with TestClient(app) as client:
        sleep(1)
        resp: Response = client.get(
            f"/cpu?seconds={seconds}&fail_rate=0&slow_rate=0&jitter_mean=0&extra_delay_prob=0",
            headers={"X-Balancer-Deadline": "2000"},
        )
        assert resp.status_code == 200

        answer = ServerCPUResponse(**resp.json())
        assert answer.cpu_burn is True
        assert (
            abs(answer.seconds - seconds * 1000.0) < 400
        )  # разница по времени менее 400мс
