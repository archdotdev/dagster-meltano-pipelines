import warnings

import pytest

from dagster_meltano_pipelines.components.meltano_pipeline.component import (
    MeltanoPipeline,
    get_all_ssh_keys,
)
from dagster_meltano_pipelines.resources import (
    Extractor,
    ExtractorConfig,
    Loader,
    LoaderConfig,
)


@pytest.fixture
def extractor_with_ssh() -> Extractor:
    """Create an extractor with SSH key for testing."""
    return Extractor(
        name="tap-github",
        config=ExtractorConfig(),  # type: ignore[call-arg]
        git_ssh_private_key="extractor-ssh-key",
    )


@pytest.fixture
def loader_with_ssh() -> Loader:
    """Create a loader with SSH key for testing."""
    return Loader(
        name="target-postgres",
        config=LoaderConfig(),  # type: ignore[call-arg]
        git_ssh_private_key="loader-ssh-key",
    )


@pytest.fixture
def extractor_no_ssh() -> Extractor:
    """Create an extractor without SSH key for testing."""
    return Extractor(
        name="tap-csv",
        config=ExtractorConfig(),  # type: ignore[call-arg]
        git_ssh_private_key=None,
    )


@pytest.fixture
def loader_no_ssh() -> Loader:
    """Create a loader without SSH key for testing."""
    return Loader(
        name="target-jsonl",
        config=LoaderConfig(),  # type: ignore[call-arg]
        git_ssh_private_key=None,
    )


def test_get_all_ssh_keys_from_plugins_only(extractor_with_ssh: Extractor, loader_with_ssh: Loader) -> None:
    """Test collecting SSH keys from plugins only (no deprecated pipeline keys)."""
    pipeline = MeltanoPipeline(
        id="test-pipeline",
        extractor=extractor_with_ssh,
        loader=loader_with_ssh,
        git_ssh_private_keys=[],
        meltano_config=None,
        state_suffix=None,  # No deprecated keys
    )

    ssh_keys = get_all_ssh_keys(pipeline)

    # Should collect keys from both extractor and loader
    assert ssh_keys == ["extractor-ssh-key", "loader-ssh-key"]


def test_get_all_ssh_keys_mixed_plugins(extractor_with_ssh: Extractor, loader_no_ssh: Loader) -> None:
    """Test collecting SSH keys when only some plugins have keys."""
    pipeline = MeltanoPipeline(
        id="test-pipeline",
        extractor=extractor_with_ssh,
        loader=loader_no_ssh,
        git_ssh_private_keys=[],
        meltano_config=None,
        state_suffix=None,
    )

    ssh_keys = get_all_ssh_keys(pipeline)

    # Should only collect keys from extractor
    assert ssh_keys == ["extractor-ssh-key"]


def test_get_all_ssh_keys_loader_only(extractor_no_ssh: Extractor, loader_with_ssh: Loader) -> None:
    """Test collecting SSH keys when only loader has a key."""
    pipeline = MeltanoPipeline(
        id="test-pipeline",
        extractor=extractor_no_ssh,
        loader=loader_with_ssh,
        git_ssh_private_keys=[],
        meltano_config=None,
        state_suffix=None,
    )

    ssh_keys = get_all_ssh_keys(pipeline)

    # Should only collect keys from loader
    assert ssh_keys == ["loader-ssh-key"]


def test_get_all_ssh_keys_no_plugins_have_keys(extractor_no_ssh: Extractor, loader_no_ssh: Loader) -> None:
    """Test collecting SSH keys when no plugins have keys."""
    pipeline = MeltanoPipeline(
        id="test-pipeline",
        extractor=extractor_no_ssh,
        loader=loader_no_ssh,
        git_ssh_private_keys=[],
        meltano_config=None,
        state_suffix=None,
    )

    ssh_keys = get_all_ssh_keys(pipeline)

    # Should return empty list
    assert ssh_keys == []


def test_get_all_ssh_keys_deprecated_pipeline_keys_only(extractor_no_ssh: Extractor, loader_no_ssh: Loader) -> None:
    """Test collecting SSH keys from deprecated pipeline-level configuration."""
    pipeline = MeltanoPipeline(
        id="test-pipeline",
        extractor=extractor_no_ssh,
        loader=loader_no_ssh,
        git_ssh_private_keys=["deprecated-key-1", "deprecated-key-2"],
        meltano_config=None,
        state_suffix=None,
    )

    # Test that deprecation warning is raised
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        ssh_keys = get_all_ssh_keys(pipeline)

        # Should have raised a deprecation warning
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)
        assert "deprecated" in str(w[0].message).lower()
        assert "git_ssh_private_keys" in str(w[0].message)

    # Should return the deprecated keys
    assert ssh_keys == ["deprecated-key-1", "deprecated-key-2"]


def test_get_all_ssh_keys_mixed_new_and_deprecated(extractor_with_ssh: Extractor, loader_with_ssh: Loader) -> None:
    """Test collecting SSH keys from both new plugin-level and deprecated pipeline-level configuration."""
    pipeline = MeltanoPipeline(
        id="test-pipeline",
        extractor=extractor_with_ssh,
        loader=loader_with_ssh,
        git_ssh_private_keys=["deprecated-key-1"],
        meltano_config=None,
        state_suffix=None,
    )

    # Test that deprecation warning is raised
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        ssh_keys = get_all_ssh_keys(pipeline)

        # Should have raised a deprecation warning
        assert len(w) == 1
        assert issubclass(w[0].category, DeprecationWarning)

    # Should collect keys from plugins first, then deprecated keys
    assert ssh_keys == ["extractor-ssh-key", "loader-ssh-key", "deprecated-key-1"]


def test_get_all_ssh_keys_deprecation_warning_message(extractor_no_ssh: Extractor, loader_no_ssh: Loader) -> None:
    """Test that the deprecation warning has the correct message."""
    pipeline = MeltanoPipeline(
        id="test-pipeline",
        extractor=extractor_no_ssh,
        loader=loader_no_ssh,
        git_ssh_private_keys=["deprecated-key"],
        meltano_config=None,
        state_suffix=None,
    )

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        get_all_ssh_keys(pipeline)

        # Check the warning message
        assert len(w) == 1
        warning_message = str(w[0].message)
        assert "Pipeline-level git_ssh_private_keys is deprecated" in warning_message
        assert "Configure git_ssh_private_key on individual extractor and loader plugins instead" in warning_message


def test_get_all_ssh_keys_no_warning_when_no_deprecated_keys(
    extractor_with_ssh: Extractor,
    loader_with_ssh: Loader,
) -> None:
    """Test that no deprecation warning is raised when only using new plugin-level configuration."""
    pipeline = MeltanoPipeline(
        id="test-pipeline",
        extractor=extractor_with_ssh,
        loader=loader_with_ssh,
        git_ssh_private_keys=[],
        meltano_config=None,
        state_suffix=None,  # No deprecated keys
    )

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        ssh_keys = get_all_ssh_keys(pipeline)

        # Should not raise any warnings
        assert len(w) == 0

    # Should collect keys from plugins
    assert ssh_keys == ["extractor-ssh-key", "loader-ssh-key"]


def test_get_all_ssh_keys_empty_deprecated_list_no_warning(
    extractor_with_ssh: Extractor,
    loader_with_ssh: Loader,
) -> None:
    """Test that no deprecation warning is raised when deprecated list is empty."""
    pipeline = MeltanoPipeline(
        id="test-pipeline",
        extractor=extractor_with_ssh,
        loader=loader_with_ssh,
        git_ssh_private_keys=[],
        meltano_config=None,
        state_suffix=None,  # Empty deprecated list
    )

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        ssh_keys = get_all_ssh_keys(pipeline)

        # Should not raise any warnings
        assert len(w) == 0

    # Should collect keys from plugins
    assert ssh_keys == ["extractor-ssh-key", "loader-ssh-key"]
