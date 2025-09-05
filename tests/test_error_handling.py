from dagster_meltano_pipelines.errors import DagsterMeltanoPipelineError


def test_dagster_meltano_pipeline_error_basic() -> None:
    """Test basic DagsterMeltanoPipelineError functionality."""
    error = DagsterMeltanoPipelineError("Test error", exit_code=1)

    assert error.exit_code == 1
    assert error.error_logs == []
    assert "Test error (exit code: 1)" in str(error)


def test_dagster_meltano_pipeline_error_with_logs() -> None:
    """Test DagsterMeltanoPipelineError with error logs."""
    error_logs = [
        "Connection failed to database",
        "{'level': 'ERROR', 'event': 'Extractor failed', 'code': 500}",
        "Authentication error",
    ]
    error = DagsterMeltanoPipelineError("Pipeline failed", exit_code=2, error_logs=error_logs)

    assert error.exit_code == 2
    assert error.error_logs == error_logs
    error_str = str(error)
    assert "Pipeline failed (exit code: 2)" in error_str
    assert "Meltano error logs:" in error_str
    assert "Connection failed to database" in error_str
    assert "Authentication error" in error_str


def test_dagster_meltano_pipeline_error_truncates_logs() -> None:
    """Test that error logs are truncated to last 5 entries."""
    error_logs = [f"Error {i}" for i in range(10)]
    error = DagsterMeltanoPipelineError("Many errors", exit_code=3, error_logs=error_logs)

    error_str = str(error)
    # Should only contain the last 5 errors
    assert "Error 5" in error_str
    assert "Error 9" in error_str
    # Should not contain the first errors
    assert "Error 0" not in error_str
    assert "Error 4" not in error_str
