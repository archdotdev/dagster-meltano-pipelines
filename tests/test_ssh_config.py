import os
import stat
import tempfile
import typing as t
from unittest.mock import Mock

import dagster as dg
import pytest
from pydantic import SecretStr

from dagster_meltano_pipelines.components.meltano_pipeline.component import setup_ssh_config


@pytest.fixture
def mock_context() -> Mock:
    """Create a mock AssetExecutionContext for testing."""
    context = Mock(spec=dg.AssetExecutionContext)
    context.run_id = "test-run-123"
    context.log = Mock()
    return context


@pytest.fixture
def sample_ssh_key() -> str:
    """Sample SSH private key content for testing."""
    return """-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAFwAAAAdzc2gtcn
NhAAAAAwEAAQAAAQEAzC6K3nHOeZQq8qbUQTSQQzYzQyMzQzMzMzNDM2NzM2NzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzMzM=
-----END OPENSSH PRIVATE KEY-----"""


@pytest.fixture
def sample_ssh_keys() -> t.List[str]:
    """Multiple SSH private keys for testing."""
    return [
        """-----BEGIN OPENSSH PRIVATE KEY-----
key1content
-----END OPENSSH PRIVATE KEY-----""",
        """-----BEGIN OPENSSH PRIVATE KEY-----
key2content
-----END OPENSSH PRIVATE KEY-----""",
    ]


def test_setup_ssh_config_empty_keys(mock_context: Mock) -> None:
    """Test that setup_ssh_config yields None when no SSH keys are provided."""
    with setup_ssh_config(mock_context, []) as ssh_config_path:
        assert ssh_config_path is None

    # Should log that SSH config is being set up
    mock_context.log.info.assert_not_called()


def test_setup_ssh_config_single_key(mock_context: Mock, sample_ssh_key: str) -> None:
    """Test SSH config setup with a single key."""
    ssh_keys = [SecretStr(sample_ssh_key)]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        assert ssh_config_path is not None
        assert os.path.exists(ssh_config_path)

        # Read and verify SSH config content
        with open(ssh_config_path, "r") as f:
            config_content = f.read()

        assert "Host *" in config_content
        assert "IdentityFile" in config_content
        assert "IdentitiesOnly yes" in config_content
        assert "StrictHostKeyChecking no" in config_content
        assert "UserKnownHostsFile /dev/null" in config_content

        # Verify temp directory structure
        temp_dir = os.path.dirname(ssh_config_path)
        assert temp_dir.startswith(tempfile.gettempdir())
        assert "meltano_ssh_" in temp_dir

        # Find the key file referenced in the config
        key_files = [f for f in os.listdir(temp_dir) if f.endswith("_id_rsa_0")]
        assert len(key_files) == 1
        key_file_path = os.path.join(temp_dir, key_files[0])

        # Verify key file permissions
        key_stat = os.stat(key_file_path)
        assert stat.filemode(key_stat.st_mode) == "-rw-------"
        assert key_stat.st_mode & 0o777 == 0o600

        # Verify key file content
        with open(key_file_path, "r") as f:
            key_content = f.read()
        assert key_content == sample_ssh_key

    # After context exit, files should be cleaned up
    assert not os.path.exists(ssh_config_path)
    assert not os.path.exists(temp_dir)

    # Should log that SSH config is being set up
    mock_context.log.info.assert_called_once_with("Setting up SSH configuration for Git authentication")


def test_setup_ssh_config_multiple_keys(mock_context: Mock, sample_ssh_keys: t.List[str]) -> None:
    """Test SSH config setup with multiple keys."""
    ssh_keys = [SecretStr(key) for key in sample_ssh_keys]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        assert ssh_config_path is not None
        assert os.path.exists(ssh_config_path)

        # Read SSH config content
        with open(ssh_config_path, "r") as f:
            config_content = f.read()

        # Should have multiple Host * entries (one per key)
        host_count = config_content.count("Host *")
        assert host_count == len(sample_ssh_keys)

        # Should have multiple IdentityFile entries
        identity_file_count = config_content.count("IdentityFile")
        assert identity_file_count == len(sample_ssh_keys)

        # Verify temp directory has correct number of key files
        temp_dir = os.path.dirname(ssh_config_path)
        key_files = [f for f in os.listdir(temp_dir) if "_id_rsa_" in f]
        assert len(key_files) == len(sample_ssh_keys)

        # Verify each key file has correct content and permissions
        for i, expected_key in enumerate(sample_ssh_keys):
            key_file = [f for f in key_files if f.endswith(f"_id_rsa_{i}")][0]
            key_file_path = os.path.join(temp_dir, key_file)

            # Check permissions
            key_stat = os.stat(key_file_path)
            assert key_stat.st_mode & 0o777 == 0o600

            # Check content
            with open(key_file_path, "r") as f:
                key_content = f.read()
            assert key_content == expected_key

    # Files should be cleaned up after context exit
    assert not os.path.exists(ssh_config_path)
    assert not os.path.exists(temp_dir)


def test_setup_ssh_config_temp_directory_isolation(mock_context: Mock, sample_ssh_key: str) -> None:
    """Test that each SSH config setup uses an isolated temporary directory."""
    ssh_keys = [SecretStr(sample_ssh_key)]
    temp_dirs = []

    # Create multiple SSH configs
    for _ in range(3):
        with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
            temp_dir = os.path.dirname(ssh_config_path)
            temp_dirs.append(temp_dir)
            assert os.path.exists(temp_dir)

    # Each should use a different temp directory
    assert len(set(temp_dirs)) == 3

    # All temp directories should be cleaned up
    for temp_dir in temp_dirs:
        assert not os.path.exists(temp_dir)


def test_setup_ssh_config_secret_str_handling(mock_context: Mock) -> None:
    """Test that SecretStr values are properly handled."""
    secret_key = SecretStr("test-secret-key-content")
    ssh_keys = [secret_key]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        temp_dir = os.path.dirname(ssh_config_path)
        key_files = [f for f in os.listdir(temp_dir) if "_id_rsa_" in f]
        key_file_path = os.path.join(temp_dir, key_files[0])

        # Verify the secret value was written to the file
        with open(key_file_path, "r") as f:
            key_content = f.read()
        assert key_content == "test-secret-key-content"


def test_setup_ssh_config_file_naming(mock_context: Mock, sample_ssh_keys: t.List[str]) -> None:
    """Test that SSH config and key files are named correctly."""
    ssh_keys = [SecretStr(key) for key in sample_ssh_keys]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        temp_dir = os.path.dirname(ssh_config_path)

        # SSH config file should have correct suffix
        assert ssh_config_path.endswith("_ssh_config")

        # Key files should be numbered correctly
        key_files = sorted([f for f in os.listdir(temp_dir) if "_id_rsa_" in f])
        expected_count = len(sample_ssh_keys)
        assert len(key_files) == expected_count

        # Verify each expected key file exists
        for i in range(expected_count):
            expected_files = [f for f in key_files if f.endswith(f"_id_rsa_{i}")]
            assert len(expected_files) == 1

        # Verify the SSH config references the correct key files
        with open(ssh_config_path, "r") as f:
            config_content = f.read()

        for key_file in key_files:
            key_file_path = os.path.join(temp_dir, key_file)
            assert key_file_path in config_content
