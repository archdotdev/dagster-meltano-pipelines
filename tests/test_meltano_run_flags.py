import pytest

from dagster_meltano_pipelines.components.meltano_pipeline.component import MeltanoRunFlags


class TestMeltanoRunFlags:
    """Test cases for MeltanoRunFlags.get_command method."""

    def test_get_command_defaults(self) -> None:
        """Test get_command with default flags."""
        flags = MeltanoRunFlags()
        command = flags.get_command(run_id="test-run-123")

        assert command == [
            "meltano",
            "run",
            "--run-id=test-run-123",
        ]

    def test_get_command_with_log_level(self) -> None:
        """Test get_command with log level."""
        flags = MeltanoRunFlags(log_level="debug")
        command = flags.get_command(run_id="test-run-123")

        assert command == [
            "meltano",
            "--log-level=debug",
            "run",
            "--run-id=test-run-123",
        ]

    def test_get_command_with_state_suffix(self) -> None:
        """Test get_command with state suffix."""
        flags = MeltanoRunFlags()
        command = flags.get_command(run_id="test-run-123", state_suffix="dev")

        assert command == [
            "meltano",
            "run",
            "--run-id=test-run-123",
            "--state-id-suffix=dev",
        ]

    def test_get_command_with_full_refresh(self) -> None:
        """Test get_command with full refresh flag."""
        flags = MeltanoRunFlags(full_refresh=True)
        command = flags.get_command(run_id="test-run-123")

        assert command == [
            "meltano",
            "run",
            "--run-id=test-run-123",
            "--full-refresh",
        ]

    def test_get_command_with_refresh_catalog(self) -> None:
        """Test get_command with refresh catalog flag."""
        flags = MeltanoRunFlags(refresh_catalog=True)
        command = flags.get_command(run_id="test-run-123")

        assert command == [
            "meltano",
            "run",
            "--run-id=test-run-123",
            "--refresh-catalog",
        ]

    def test_get_command_with_state_strategy(self) -> None:
        """Test get_command with different state strategies."""
        # Test merge strategy
        flags = MeltanoRunFlags(state_strategy="merge")
        command = flags.get_command(run_id="test-run-123")

        assert command == [
            "meltano",
            "run",
            "--run-id=test-run-123",
            "--state-strategy=merge",
        ]

        # Test overwrite strategy
        flags = MeltanoRunFlags(state_strategy="overwrite")
        command = flags.get_command(run_id="test-run-123")

        assert command == [
            "meltano",
            "run",
            "--run-id=test-run-123",
            "--state-strategy=overwrite",
        ]

        # Test auto strategy (should not appear in command)
        flags = MeltanoRunFlags(state_strategy="auto")
        command = flags.get_command(run_id="test-run-123")

        assert command == [
            "meltano",
            "run",
            "--run-id=test-run-123",
        ]

    def test_get_command_all_flags(self) -> None:
        """Test get_command with all flags enabled."""
        flags = MeltanoRunFlags(log_level="info", full_refresh=True, refresh_catalog=True, state_strategy="merge")
        command = flags.get_command(run_id="test-run-123", state_suffix="prod")

        assert command == [
            "meltano",
            "--log-level=info",
            "run",
            "--run-id=test-run-123",
            "--state-id-suffix=prod",
            "--full-refresh",
            "--refresh-catalog",
            "--state-strategy=merge",
        ]

    @pytest.mark.parametrize("log_level", ["debug", "info", "warning", "error", "critical"])
    def test_get_command_all_log_levels(self, log_level: str) -> None:
        """Test get_command with all supported log levels."""
        flags = MeltanoRunFlags(log_level=log_level)
        command = flags.get_command(run_id="test-run-123")

        assert f"--log-level={log_level}" in command
        assert command == [
            "meltano",
            f"--log-level={log_level}",
            "run",
            "--run-id=test-run-123",
        ]
