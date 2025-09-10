from typing import Any, Dict

import dagster as dg


def test_failure_basic() -> None:
    """Test basic Failure functionality with metadata."""
    metadata: Dict[str, Any] = {
        "exit_code": 1,
        "error_log_count": 0,
    }

    error = dg.Failure(
        description="Meltano job failed with exit code 1",
        metadata=metadata,
    )

    assert "Meltano job failed with exit code 1" in str(error)


def test_failure_with_logs() -> None:
    """Test Failure with error logs in metadata as JSON."""
    import json

    error_logs = [
        "Connection failed to database",  # Raw text error
        {"level": "ERROR", "event": "Extractor failed", "code": 500},  # Structured error
        "Authentication error",  # Raw text error
    ]

    metadata: Dict[str, Any] = {
        "exit_code": 2,
        "error_log_count": len(error_logs),
        "last_5_error_logs_json": json.dumps(error_logs[-5:], indent=2),
    }

    error = dg.Failure(
        description="Meltano job failed with exit code 2",
        metadata=metadata,
    )

    assert "Meltano job failed with exit code 2" in str(error)

    # Verify JSON can be parsed back
    parsed_logs = json.loads(metadata["last_5_error_logs_json"])
    assert len(parsed_logs) == 3
    assert parsed_logs[0] == "Connection failed to database"
    assert parsed_logs[1] == {"level": "ERROR", "event": "Extractor failed", "code": 500}
