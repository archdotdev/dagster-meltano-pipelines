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
    error_logs = [
        "Connection failed to database",  # Raw text error
        {"level": "ERROR", "event": "Extractor failed", "code": 500},  # Structured error
        "Authentication error",  # Raw text error
    ]

    metadata: Dict[str, Any] = {
        "exit_code": 2,
        "error_logs": error_logs[-5:],
    }

    error = dg.Failure(
        description="Meltano job failed with exit code 2",
        metadata=metadata,
    )

    assert "Meltano job failed with exit code 2" in str(error)
