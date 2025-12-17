"""Tests for process_meltano_stdout function."""

from unittest.mock import MagicMock

import dagster as dg
import orjson
import pytest

from dagster_meltano_pipelines.components.meltano_pipeline.component import (
    process_meltano_stdout,
)


@pytest.fixture
def mock_context() -> MagicMock:
    """Create a mock Dagster execution context."""
    context = MagicMock(spec=dg.AssetExecutionContext)
    context.log = MagicMock(spec=dg.DagsterLogManager)
    context.add_asset_metadata = MagicMock()
    return context


def test_process_json_log_info(mock_context: MagicMock) -> None:
    """Test processing JSON log with info level."""
    log_line = orjson.dumps({"level": "info", "event": "Processing started"})
    lines = [log_line]

    result = process_meltano_stdout(mock_context, lines)

    mock_context.log.log.assert_called_once_with("info", "Processing started")
    assert result.error_logs == []
    assert result.warning_logs == []
    assert result.duration_seconds is None


def test_process_json_log_error(mock_context: MagicMock) -> None:
    """Test processing JSON log with error level."""
    log_line = orjson.dumps(
        {
            "level": "error",
            "event": "Connection failed",
            "error_code": 500,
            "details": "Network timeout",
        },
    )
    lines = [log_line]

    result = process_meltano_stdout(mock_context, lines)

    mock_context.log.log.assert_called_once_with("error", "Connection failed")
    assert len(result.error_logs) == 1
    assert result.error_logs[0] == {
        "level": "error",
        "event": "Connection failed",
        "error_code": 500,
        "details": "Network timeout",
    }
    assert result.warning_logs == []


def test_process_non_json_text(mock_context: MagicMock) -> None:
    """Test processing non-JSON text output."""
    lines = [b"Plain text log message\n"]

    result = process_meltano_stdout(mock_context, lines)

    mock_context.log.info.assert_called_once_with("Plain text log message")
    assert result.error_logs == []
    assert result.warning_logs == []


def test_process_non_json_error_text(mock_context: MagicMock) -> None:
    """Test processing non-JSON text containing 'error'."""
    lines = [b"Error: Something went wrong\n"]

    result = process_meltano_stdout(mock_context, lines)

    mock_context.log.info.assert_called_once_with("Error: Something went wrong")
    assert len(result.error_logs) == 1
    assert result.error_logs[0] == "Error: Something went wrong"
    assert result.warning_logs == []


def test_process_metric_log_with_metric_info(mock_context: MagicMock) -> None:
    """Test processing METRIC log with metric_info."""
    log_line = orjson.dumps(
        {
            "level": "info",
            "event": "METRIC",
            "metric_info": {
                "metric_type": "counter",
                "value": 1000,
                "stream": "users",
            },
        },
    )
    result = process_meltano_stdout(mock_context, [log_line])

    assert result.error_logs == []
    assert result.warning_logs == []
    assert result.duration_seconds is None

    # Should call debug with formatted metric info
    mock_context.log.debug.assert_called_once()
    debug_call_arg = mock_context.log.debug.call_args[0][0]
    assert "METRIC: metric_type='counter' value=1000 stream='users'" in debug_call_arg


def test_process_metric_log_without_metric_info(mock_context: MagicMock) -> None:
    """Test processing METRIC log without metric_info."""
    log_line = orjson.dumps(
        {
            "level": "info",
            "event": "METRIC",
        },
    )
    result = process_meltano_stdout(mock_context, [log_line])

    assert result.error_logs == []
    assert result.warning_logs == []
    assert result.duration_seconds is None

    # Should call debug with the raw event
    mock_context.log.debug.assert_called_once_with("METRIC")


def test_process_run_completed_log(mock_context: MagicMock) -> None:
    """Test processing 'Run completed' log with duration."""
    log_line = orjson.dumps(
        {
            "level": "info",
            "event": "Run completed successfully",
            "duration_seconds": 45.67,
        },
    )
    lines = [log_line]

    result = process_meltano_stdout(mock_context, lines)

    mock_context.log.log.assert_called_once_with("info", "Run completed successfully")
    mock_context.add_asset_metadata.assert_called_once_with({"duration_seconds": 45.67})
    assert result.duration_seconds == 45.67
    assert result.warning_logs == []


def test_process_mixed_logs(mock_context: MagicMock) -> None:
    """Test processing a mix of JSON and non-JSON logs."""
    lines = [
        b"Starting process...\n",
        orjson.dumps({"level": "info", "event": "Connecting to database"}),
        orjson.dumps({"level": "error", "event": "Authentication failed", "code": 401}),
        b"Error: Retrying connection\n",
        orjson.dumps({"level": "info", "event": "Run completed", "duration_seconds": 12.34}),
    ]

    result = process_meltano_stdout(mock_context, lines)

    # Check that all logs were processed
    assert mock_context.log.info.call_count == 2  # Two non-JSON logs
    assert mock_context.log.log.call_count == 3  # Three JSON logs

    # Check error logs collection
    assert len(result.error_logs) == 2
    assert result.error_logs[0] == {
        "level": "error",
        "event": "Authentication failed",
        "code": 401,
    }
    assert result.error_logs[1] == "Error: Retrying connection"

    # Check duration
    assert result.duration_seconds == 12.34

    # Check warning logs
    assert result.warning_logs == []


def test_process_empty_lines(mock_context: MagicMock) -> None:
    """Test processing empty input."""
    lines: list[bytes] = []

    result = process_meltano_stdout(mock_context, lines)

    assert result.error_logs == []
    assert result.warning_logs == []
    assert result.duration_seconds is None
    mock_context.log.info.assert_not_called()
    mock_context.log.log.assert_not_called()


def test_process_malformed_json(mock_context: MagicMock) -> None:
    """Test processing malformed JSON falls back to text logging."""
    lines = [b'{"level": "info", "event": incomplete\n']

    result = process_meltano_stdout(mock_context, lines)

    mock_context.log.info.assert_called_once()
    assert result.error_logs == []
    assert result.warning_logs == []


def test_process_case_insensitive_error_detection(mock_context: MagicMock) -> None:
    """Test that error detection is case-insensitive."""
    lines = [
        b"ERROR: Connection refused\n",
        b"error: timeout occurred\n",
        b"ErRoR: invalid input\n",
    ]

    result = process_meltano_stdout(mock_context, lines)

    assert len(result.error_logs) == 3
    assert result.error_logs[0] == "ERROR: Connection refused"
    assert result.error_logs[1] == "error: timeout occurred"
    assert result.error_logs[2] == "ErRoR: invalid input"
    assert result.warning_logs == []


def test_process_multiple_run_completed_logs(mock_context: MagicMock) -> None:
    """Test that the last 'Run completed' duration is used."""
    lines = [
        orjson.dumps({"level": "info", "event": "Run completed", "duration_seconds": 10.0}),
        orjson.dumps({"level": "info", "event": "Run completed", "duration_seconds": 20.0}),
    ]

    result = process_meltano_stdout(mock_context, lines)

    # Should have added metadata twice
    assert mock_context.add_asset_metadata.call_count == 2
    # Result should contain the last duration
    assert result.duration_seconds == 20.0
    assert result.warning_logs == []


def test_process_error_log_preserves_extra_fields(mock_context: MagicMock) -> None:
    """Test that error logs preserve all extra fields."""
    log_line = orjson.dumps(
        {
            "level": "error",
            "event": "Pipeline failed",
            "timestamp": "2024-01-01T00:00:00Z",
            "extractor": "tap-postgres",
            "loader": "target-jsonl",
            "exception": "ValueError",
            "traceback": "...",
        },
    )
    lines = [log_line]

    result = process_meltano_stdout(mock_context, lines)

    assert len(result.error_logs) == 1
    error_log = result.error_logs[0]
    assert isinstance(error_log, dict)
    assert error_log["level"] == "error"
    assert error_log["event"] == "Pipeline failed"
    assert error_log["timestamp"] == "2024-01-01T00:00:00Z"
    assert error_log["extractor"] == "tap-postgres"
    assert error_log["loader"] == "target-jsonl"
    assert error_log["exception"] == "ValueError"
    assert result.warning_logs == []


def test_process_unicode_characters(mock_context: MagicMock) -> None:
    """Test processing logs with unicode characters."""
    lines = [
        "Processing user: João 🎉\n".encode(),
        orjson.dumps({"level": "info", "event": "Processed user: 用户"}),
    ]
    result = process_meltano_stdout(mock_context, lines)
    assert result.error_logs == []
    assert result.warning_logs == []
    assert result.duration_seconds is None

    mock_context.log.info.assert_called_once_with("Processing user: João 🎉")
    mock_context.log.log.assert_called_once_with("info", "Processed user: 用户")


def test_process_json_log_warning(mock_context: MagicMock) -> None:
    """Test processing JSON log with warning level."""
    log_line = orjson.dumps(
        {
            "level": "warning",
            "event": "Deprecated feature used",
            "feature": "old_api",
            "suggestion": "Use new_api instead",
        },
    )
    lines = [log_line]

    result = process_meltano_stdout(mock_context, lines)

    mock_context.log.log.assert_called_once_with("warning", "Deprecated feature used")
    assert len(result.warning_logs) == 1
    assert result.warning_logs[0] == {
        "level": "warning",
        "event": "Deprecated feature used",
        "feature": "old_api",
        "suggestion": "Use new_api instead",
    }
    assert result.error_logs == []


def test_process_warning_log_preserves_extra_fields(mock_context: MagicMock) -> None:
    """Test that warning logs preserve all extra fields."""
    log_line = orjson.dumps(
        {
            "level": "warning",
            "event": "Schema mismatch detected",
            "timestamp": "2024-01-01T00:00:00Z",
            "extractor": "tap-postgres",
            "stream": "users",
            "expected_type": "integer",
            "actual_type": "string",
        },
    )
    lines = [log_line]

    result = process_meltano_stdout(mock_context, lines)

    assert len(result.warning_logs) == 1
    warning_log = result.warning_logs[0]
    assert isinstance(warning_log, dict)
    assert warning_log["level"] == "warning"
    assert warning_log["event"] == "Schema mismatch detected"
    assert warning_log["timestamp"] == "2024-01-01T00:00:00Z"
    assert warning_log["extractor"] == "tap-postgres"
    assert warning_log["stream"] == "users"
    assert warning_log["expected_type"] == "integer"
    assert warning_log["actual_type"] == "string"
    assert result.error_logs == []


def test_process_mixed_logs_with_warnings(mock_context: MagicMock) -> None:
    """Test processing a mix of logs including warnings, errors, and info."""
    lines = [
        orjson.dumps({"level": "info", "event": "Starting pipeline"}),
        orjson.dumps({"level": "warning", "event": "Slow query detected", "duration_ms": 5000}),
        orjson.dumps({"level": "info", "event": "Processing records"}),
        orjson.dumps({"level": "warning", "event": "Rate limit approaching", "remaining": 10}),
        orjson.dumps({"level": "error", "event": "Connection timeout", "code": 408}),
        orjson.dumps({"level": "info", "event": "Run completed", "duration_seconds": 30.5}),
    ]

    result = process_meltano_stdout(mock_context, lines)

    # Check that all logs were processed
    assert mock_context.log.log.call_count == 6

    # Check warning logs collection
    assert len(result.warning_logs) == 2
    assert result.warning_logs[0] == {
        "level": "warning",
        "event": "Slow query detected",
        "duration_ms": 5000,
    }
    assert result.warning_logs[1] == {
        "level": "warning",
        "event": "Rate limit approaching",
        "remaining": 10,
    }

    # Check error logs collection
    assert len(result.error_logs) == 1
    assert result.error_logs[0] == {
        "level": "error",
        "event": "Connection timeout",
        "code": 408,
    }

    # Check duration
    assert result.duration_seconds == 30.5
