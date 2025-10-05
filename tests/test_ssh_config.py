import os
import stat
import tempfile
import typing as t
from unittest.mock import Mock

import dagster as dg
import pytest

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


@pytest.fixture
def env_ssh_key_content() -> str:
    """SSH key content for environment variable testing."""
    return """-----BEGIN OPENSSH PRIVATE KEY-----
test-key-from-env-var
-----END OPENSSH PRIVATE KEY-----"""


@pytest.fixture
def string_ssh_key() -> str:
    """SSH key for mixed type testing."""
    return """-----BEGIN OPENSSH PRIVATE KEY-----
string-key-content
-----END OPENSSH PRIVATE KEY-----"""


@pytest.fixture
def env_ssh_key_content_mixed() -> str:
    """SSH key content for mixed type environment variable testing."""
    return """-----BEGIN OPENSSH PRIVATE KEY-----
env-key-content
-----END OPENSSH PRIVATE KEY-----"""


@pytest.fixture
def ssh_key_with_literal_newlines() -> str:
    """SSH key with literal \\n characters for newline replacement testing."""
    return "-----BEGIN OPENSSH PRIVATE KEY-----\\nb3BlbnNzaC1rZXktdjE=\\n-----END OPENSSH PRIVATE KEY-----"


@pytest.fixture
def expected_ssh_key_with_real_newlines() -> str:
    """Expected SSH key content after literal newline replacement."""
    return "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjE=\n-----END OPENSSH PRIVATE KEY-----\n"


@pytest.fixture
def secret_key_content() -> str:
    """Simple secret key content for testing."""
    return "test-secret-key-content"


def test_setup_ssh_config_empty_keys(mock_context: Mock) -> None:
    """Test that setup_ssh_config yields None when no SSH keys are provided."""
    with setup_ssh_config(mock_context, []) as ssh_config_path:
        assert ssh_config_path is None

    # Should log that SSH config is being set up
    mock_context.log.info.assert_not_called()


def test_setup_ssh_config_single_key(mock_context: Mock, sample_ssh_key: str) -> None:
    """Test SSH config setup with a single key."""
    ssh_keys = [sample_ssh_key]

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

        # Verify key file content (should have trailing newline added if missing)
        with open(key_file_path, "r") as f:
            key_content = f.read()
        expected_content = sample_ssh_key if sample_ssh_key.endswith("\n") else sample_ssh_key + "\n"
        assert key_content == expected_content

    # After context exit, files should be cleaned up
    assert not os.path.exists(ssh_config_path)
    assert not os.path.exists(temp_dir)

    # Should log that SSH config is being set up
    mock_context.log.info.assert_called_once_with("Setting up SSH configuration for Git authentication")


def test_setup_ssh_config_multiple_keys(mock_context: Mock, sample_ssh_keys: t.List[str]) -> None:
    """Test SSH config setup with multiple keys."""
    ssh_keys = sample_ssh_keys

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

            # Check content (should have trailing newline added if missing)
            with open(key_file_path, "r") as f:
                key_content = f.read()
            expected_content = expected_key if expected_key.endswith("\n") else expected_key + "\n"
            assert key_content == expected_content

    # Files should be cleaned up after context exit
    assert not os.path.exists(ssh_config_path)
    assert not os.path.exists(temp_dir)


def test_setup_ssh_config_temp_directory_isolation(mock_context: Mock, sample_ssh_key: str) -> None:
    """Test that each SSH config setup uses an isolated temporary directory."""
    ssh_keys = [sample_ssh_key]
    temp_dirs = []

    # Create multiple SSH configs
    for _ in range(3):
        with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
            assert ssh_config_path is not None
            temp_dir = os.path.dirname(ssh_config_path)
            temp_dirs.append(temp_dir)
            assert os.path.exists(temp_dir)

    # Each should use a different temp directory
    assert len(set(temp_dirs)) == 3

    # All temp directories should be cleaned up
    for temp_dir in temp_dirs:
        assert not os.path.exists(temp_dir)


def test_setup_ssh_config_secret_str_handling(mock_context: Mock, secret_key_content: str) -> None:
    """Test that SecretStr values are properly handled."""
    ssh_keys = [secret_key_content]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        assert ssh_config_path is not None
        temp_dir = os.path.dirname(ssh_config_path)
        key_files = [f for f in os.listdir(temp_dir) if "_id_rsa_" in f]
        key_file_path = os.path.join(temp_dir, key_files[0])

        # Verify the secret value was written to the file (should have trailing newline added)
        with open(key_file_path, "r") as f:
            key_content = f.read()
        assert key_content == secret_key_content + "\n"


def test_setup_ssh_config_file_naming(mock_context: Mock, sample_ssh_keys: t.List[str]) -> None:
    """Test that SSH config and key files are named correctly."""
    ssh_keys = sample_ssh_keys

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        assert ssh_config_path is not None
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


def test_setup_ssh_config_literal_newline_replacement(
    mock_context: Mock, ssh_key_with_literal_newlines: str, expected_ssh_key_with_real_newlines: str
) -> None:
    """Test that literal \\n strings are replaced with actual newlines in SSH keys."""
    ssh_keys = [ssh_key_with_literal_newlines]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        assert ssh_config_path is not None
        temp_dir = os.path.dirname(ssh_config_path)

        # Find the key file
        key_files = [f for f in os.listdir(temp_dir) if "_id_rsa_" in f]
        assert len(key_files) == 1
        key_file_path = os.path.join(temp_dir, key_files[0])

        # Verify the literal \n was replaced with actual newlines
        with open(key_file_path, "r") as f:
            key_content = f.read()
        assert key_content == expected_ssh_key_with_real_newlines

        # Verify it contains actual newlines, not literal \n
        assert "\\n" not in key_content
        assert "\n" in key_content


def test_setup_ssh_config_env_var_not_set(mock_context: Mock) -> None:
    """Test SSH config setup with unset EnvVar raises appropriate error."""
    # Create SSH keys list with unset EnvVar
    ssh_keys = [dg.EnvVar("UNSET_SSH_KEY")]

    with pytest.raises(ValueError, match=r"Environment variable for SSH key at index 0 is not set"):
        with setup_ssh_config(mock_context, ssh_keys):
            pass


def test_setup_ssh_config_with_env_var(
    mock_context: Mock, monkeypatch: pytest.MonkeyPatch, env_ssh_key_content: str
) -> None:
    """Test SSH config setup with dg.EnvVar values."""
    # Set up environment variable
    monkeypatch.setenv("SSH_PRIVATE_KEY", env_ssh_key_content)

    # Create SSH keys list with EnvVar
    ssh_keys = [dg.EnvVar("SSH_PRIVATE_KEY")]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        assert ssh_config_path is not None
        assert os.path.exists(ssh_config_path)

        # Read and verify SSH config content
        with open(ssh_config_path, "r") as f:
            config_content = f.read()

        assert "Host *" in config_content
        assert "IdentityFile" in config_content
        assert "IdentitiesOnly yes" in config_content

        # Find the key file referenced in the config
        temp_dir = os.path.dirname(ssh_config_path)
        key_files = [f for f in os.listdir(temp_dir) if f.endswith("_id_rsa_0")]
        assert len(key_files) == 1
        key_file_path = os.path.join(temp_dir, key_files[0])

        # Verify key file content (should have trailing newline added)
        with open(key_file_path, "r") as f:
            key_content = f.read()
        expected_content = env_ssh_key_content + "\n"
        assert key_content == expected_content


def test_setup_ssh_config_mixed_types(
    mock_context: Mock, monkeypatch: pytest.MonkeyPatch, string_ssh_key: str, env_ssh_key_content_mixed: str
) -> None:
    """Test SSH config setup with mixed string and EnvVar values."""
    # Set up environment variable
    monkeypatch.setenv("SSH_ENV_KEY", env_ssh_key_content_mixed)

    # Create SSH keys list with mixed types
    ssh_keys = [string_ssh_key, dg.EnvVar("SSH_ENV_KEY")]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        assert ssh_config_path is not None
        assert os.path.exists(ssh_config_path)

        # Verify temp directory has correct number of key files
        temp_dir = os.path.dirname(ssh_config_path)
        key_files = [f for f in os.listdir(temp_dir) if "_id_rsa_" in f]
        assert len(key_files) == 2

        # Verify each key file has correct content
        for i, expected_key in enumerate([string_ssh_key, env_ssh_key_content_mixed]):
            key_file = [f for f in key_files if f.endswith(f"_id_rsa_{i}")][0]
            key_file_path = os.path.join(temp_dir, key_file)

            # Check content (should have trailing newline added if missing)
            with open(key_file_path, "r") as f:
                key_content = f.read()
            expected_content = expected_key if expected_key.endswith("\n") else expected_key + "\n"
            assert key_content == expected_content
