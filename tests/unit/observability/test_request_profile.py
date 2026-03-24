from modules.observability.application.services.request_profile import (
    build_request_profile,
)


def test_build_request_profile_for_cpu_with_seconds_bucket():
    profile = build_request_profile("/cpu", {"seconds": "2.9"})
    assert profile == "cpu:2"


def test_build_request_profile_for_cpu_without_seconds():
    profile = build_request_profile("/cpu", {})
    assert profile == "cpu:unknown"


def test_build_request_profile_for_mem_low_bucket():
    profile = build_request_profile("/mem", {"seconds": "5", "mb": "64"})
    assert profile == "mem:5:low"


def test_build_request_profile_for_mem_mid_bucket():
    profile = build_request_profile("/mem", {"seconds": "5", "mb": "256"})
    assert profile == "mem:5:mid"


def test_build_request_profile_for_mem_high_bucket():
    profile = build_request_profile("/mem", {"seconds": "5", "mb": "1024"})
    assert profile == "mem:5:high"


def test_build_request_profile_for_mem_invalid_mb_defaults_to_high():
    profile = build_request_profile("/mem", {"seconds": "5", "mb": "abc"})
    assert profile == "mem:5:high"


def test_build_request_profile_for_unknown_endpoint_uses_first_path_part():
    profile = build_request_profile("/api/v1/tasks", {"seconds": "1"})
    assert profile == "api"


def test_build_request_profile_for_root_path():
    profile = build_request_profile("/", {})
    assert profile == "root"


def test_build_request_profile_for_empty_path():
    profile = build_request_profile("", {})
    assert profile == "unknown"
