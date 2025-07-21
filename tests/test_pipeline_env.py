from unittest.mock import Mock

import pytest

from dagster_meltano_pipelines.components.meltano_pipeline.component import (
    MeltanoPipeline,
    build_pipeline_env,
)
from dagster_meltano_pipelines.project import MeltanoProject
from dagster_meltano_pipelines.resources import (
    CLIConfig,
    Extractor,
    ExtractorConfig,
    Loader,
    LoaderConfig,
    MeltanoConfig,
    StateBackendConfig,
)


@pytest.fixture
def mock_project() -> MeltanoProject:
    """Create a mock MeltanoProject for testing."""
    project = Mock(spec=MeltanoProject)
    project.project_dir = "/test/meltano/project"
    return project


@pytest.fixture
def simple_extractor() -> Extractor:
    """Create a simple extractor for testing."""
    return Extractor(
        name="tap-test",
        config=ExtractorConfig(  # type: ignore[call-arg]
            api_key="secret123",
            base_url="https://api.test.com",
        ),
    )


@pytest.fixture
def simple_loader() -> Loader:
    """Create a simple loader for testing."""
    return Loader(
        name="target-test",
        config=LoaderConfig(  # type: ignore[call-arg]
            host="db.test.com",
            port=5432,
            database="testdb",
        ),
    )


@pytest.fixture
def meltano_config() -> MeltanoConfig:
    """Create a MeltanoConfig for testing."""
    return MeltanoConfig(
        state_backend=StateBackendConfig(
            uri="s3://test-bucket/state",
        ),
        cli=CLIConfig(log_level="info", log_format="json"),
    )


@pytest.fixture
def simple_pipeline(simple_extractor: Extractor, simple_loader: Loader) -> MeltanoPipeline:
    """Create a simple pipeline for testing."""
    return MeltanoPipeline(
        id="test-pipeline",
        extractor=simple_extractor,
        loader=simple_loader,
        description="Test pipeline",
        env={"CUSTOM_VAR": "custom_value"},
        meltano_config=None,
        state_suffix=None,
    )


@pytest.fixture
def pipeline_with_config(
    simple_extractor: Extractor, simple_loader: Loader, meltano_config: MeltanoConfig
) -> MeltanoPipeline:
    """Create a pipeline with Meltano config for testing."""
    return MeltanoPipeline(
        id="test-pipeline-with-config",
        extractor=simple_extractor,
        loader=simple_loader,
        meltano_config=meltano_config,
        env={"PIPELINE_ENV": "pipeline_value"},
        state_suffix=None,
    )


def test_build_pipeline_env_basic(simple_pipeline: MeltanoPipeline, mock_project: MeltanoProject) -> None:
    """Test basic environment variable building."""
    base_env = {"EXISTING_VAR": "existing_value", "PATH": "/usr/bin"}

    result = build_pipeline_env(simple_pipeline, mock_project, base_env=base_env)

    # Should contain base environment
    assert result["EXISTING_VAR"] == "existing_value"
    assert result["PATH"] == "/usr/bin"

    # Should contain extractor variables
    assert result["TAP_TEST_API_KEY"] == "secret123"
    assert result["TAP_TEST_BASE_URL"] == "https://api.test.com"

    # Should contain loader variables
    assert result["TARGET_TEST_HOST"] == "db.test.com"
    assert result["TARGET_TEST_PORT"] == "5432"
    assert result["TARGET_TEST_DATABASE"] == "testdb"

    # Should contain pipeline env variables
    assert result["CUSTOM_VAR"] == "custom_value"


def test_build_pipeline_env_removes_meltano_project_root(
    simple_pipeline: MeltanoPipeline, mock_project: MeltanoProject
) -> None:
    """Test that MELTANO_PROJECT_ROOT is removed from environment."""
    base_env = {
        "MELTANO_PROJECT_ROOT": "/some/other/path",
        "OTHER_VAR": "other_value",
    }

    result = build_pipeline_env(simple_pipeline, mock_project, base_env=base_env)

    # MELTANO_PROJECT_ROOT should be removed
    assert "MELTANO_PROJECT_ROOT" not in result

    # Other variables should remain
    assert result["OTHER_VAR"] == "other_value"


def test_build_pipeline_env_with_meltano_config(
    pipeline_with_config: MeltanoPipeline, mock_project: MeltanoProject
) -> None:
    """Test environment building with Meltano config."""
    base_env = {"BASE_VAR": "base_value"}

    result = build_pipeline_env(pipeline_with_config, mock_project, base_env=base_env)

    # Should contain base environment
    assert result["BASE_VAR"] == "base_value"

    # Should contain Meltano config variables
    assert result["MELTANO_STATE_BACKEND_URI"] == "s3://test-bucket/state"
    assert result["MELTANO_CLI_LOG_LEVEL"] == "info"
    assert result["MELTANO_CLI_LOG_FORMAT"] == "json"

    # Should contain extractor, loader, and pipeline env variables
    assert result["TAP_TEST_API_KEY"] == "secret123"
    assert result["TARGET_TEST_HOST"] == "db.test.com"
    assert result["PIPELINE_ENV"] == "pipeline_value"


def test_build_pipeline_env_with_ssh_config(simple_pipeline: MeltanoPipeline, mock_project: MeltanoProject) -> None:
    """Test environment building with SSH config path."""
    ssh_config_path = "/tmp/ssh_config"
    base_env = {"BASE_VAR": "base_value"}

    result = build_pipeline_env(simple_pipeline, mock_project, ssh_config_path=ssh_config_path, base_env=base_env)

    # Should contain SSH config
    assert result["GIT_SSH_COMMAND"] == f"ssh -F {ssh_config_path}"

    # Should contain other variables
    assert result["BASE_VAR"] == "base_value"
    assert result["TAP_TEST_API_KEY"] == "secret123"
    assert result["CUSTOM_VAR"] == "custom_value"


def test_build_pipeline_env_defaults_to_os_environ(
    simple_pipeline: MeltanoPipeline, mock_project: MeltanoProject, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that base_env defaults to os.environ."""
    # Set an environment variable
    monkeypatch.setenv("TEST_OS_VAR", "test_value")

    result = build_pipeline_env(simple_pipeline, mock_project)

    # Should contain the OS environment variable
    assert result["TEST_OS_VAR"] == "test_value"

    # Should contain pipeline variables
    assert result["TAP_TEST_API_KEY"] == "secret123"
    assert result["CUSTOM_VAR"] == "custom_value"


def test_build_pipeline_env_variable_precedence(
    pipeline_with_config: MeltanoPipeline, mock_project: MeltanoProject
) -> None:
    """Test that variables are applied in the correct precedence order."""
    # Create a variable that appears in multiple sources
    pipeline_with_config.env["OVERRIDE_VAR"] = "pipeline_value"

    base_env = {"OVERRIDE_VAR": "base_value"}

    result = build_pipeline_env(pipeline_with_config, mock_project, base_env=base_env)

    # Pipeline env should have highest precedence over base env
    assert result["OVERRIDE_VAR"] == "pipeline_value"

    # Should contain variables from all sources
    assert result["TAP_TEST_API_KEY"] == "secret123"  # extractor
    assert result["TARGET_TEST_HOST"] == "db.test.com"  # loader
    assert result["PIPELINE_ENV"] == "pipeline_value"  # pipeline


def test_build_pipeline_env_no_ssh_config(simple_pipeline: MeltanoPipeline, mock_project: MeltanoProject) -> None:
    """Test environment building without SSH config."""
    base_env = {"BASE_VAR": "base_value"}

    result = build_pipeline_env(simple_pipeline, mock_project, base_env=base_env)

    # Should not contain GIT_SSH_COMMAND
    assert "GIT_SSH_COMMAND" not in result

    # Should contain other variables
    assert result["BASE_VAR"] == "base_value"
    assert result["TAP_TEST_API_KEY"] == "secret123"


def test_build_pipeline_env_no_meltano_config(simple_pipeline: MeltanoPipeline, mock_project: MeltanoProject) -> None:
    """Test environment building without Meltano config."""
    base_env = {"BASE_VAR": "base_value"}

    result = build_pipeline_env(simple_pipeline, mock_project, base_env=base_env)

    # Should only contain default log format, no other Meltano config variables
    meltano_vars = {key: value for key, value in result.items() if key.startswith("MELTANO_")}
    assert meltano_vars == {"MELTANO_CLI_LOG_FORMAT": "json"}

    # Should contain other variables
    assert result["BASE_VAR"] == "base_value"
    assert result["TAP_TEST_API_KEY"] == "secret123"
    assert result["CUSTOM_VAR"] == "custom_value"


def test_build_pipeline_env_empty_base_env(simple_pipeline: MeltanoPipeline, mock_project: MeltanoProject) -> None:
    """Test environment building with empty base environment."""
    base_env: dict[str, str] = {}

    result = build_pipeline_env(simple_pipeline, mock_project, base_env=base_env)

    # Should contain extractor, loader, and pipeline variables
    assert result["TAP_TEST_API_KEY"] == "secret123"
    assert result["TARGET_TEST_HOST"] == "db.test.com"
    assert result["CUSTOM_VAR"] == "custom_value"

    # Should contain expected variables including default log format
    expected_keys = {
        "TAP_TEST_API_KEY",
        "TAP_TEST_BASE_URL",
        "TARGET_TEST_HOST",
        "TARGET_TEST_PORT",
        "TARGET_TEST_DATABASE",
        "CUSTOM_VAR",
        "MELTANO_CLI_LOG_FORMAT",
    }
    assert set(result.keys()) == expected_keys
    assert result["MELTANO_CLI_LOG_FORMAT"] == "json"


def test_build_pipeline_env_preserves_base_env_copy(
    simple_pipeline: MeltanoPipeline, mock_project: MeltanoProject
) -> None:
    """Test that the original base_env dict is not modified."""
    base_env: dict[str, str] = {"ORIGINAL_VAR": "original_value", "MELTANO_PROJECT_ROOT": "remove_me"}
    original_base_env = base_env.copy()

    result = build_pipeline_env(simple_pipeline, mock_project, base_env=base_env)

    # Original base_env should not be modified
    assert base_env == original_base_env

    # Result should have MELTANO_PROJECT_ROOT removed
    assert "MELTANO_PROJECT_ROOT" not in result
    assert result["ORIGINAL_VAR"] == "original_value"


def test_build_pipeline_env_respects_existing_log_format(
    simple_pipeline: MeltanoPipeline, mock_project: MeltanoProject
) -> None:
    """Test that existing MELTANO_CLI_LOG_FORMAT is not overridden."""
    base_env = {"MELTANO_CLI_LOG_FORMAT": "text"}

    result = build_pipeline_env(simple_pipeline, mock_project, base_env=base_env)

    # Should preserve existing log format
    assert result["MELTANO_CLI_LOG_FORMAT"] == "text"
