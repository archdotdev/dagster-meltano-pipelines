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


def test_setup_ssh_config_secret_str_handling(mock_context: Mock) -> None:
    """Test that SecretStr values are properly handled."""
    secret_key = "test-secret-key-content"
    ssh_keys = [secret_key]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        assert ssh_config_path is not None
        temp_dir = os.path.dirname(ssh_config_path)
        key_files = [f for f in os.listdir(temp_dir) if "_id_rsa_" in f]
        key_file_path = os.path.join(temp_dir, key_files[0])

        # Verify the secret value was written to the file (should have trailing newline added)
        with open(key_file_path, "r") as f:
            key_content = f.read()
        assert key_content == "test-secret-key-content\n"


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


def test_setup_ssh_config_literal_newline_replacement(mock_context: Mock) -> None:
    """Test that literal \\n strings are replaced with actual newlines in SSH keys."""
    # SSH key with literal \n characters instead of actual newlines
    ssh_key_with_literals = (
        "-----BEGIN OPENSSH PRIVATE KEY-----\\nb3BlbnNzaC1rZXktdjE=\\n-----END OPENSSH PRIVATE KEY-----"
    )
    expected_key_content = (
        "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjE=\n-----END OPENSSH PRIVATE KEY-----\n"
    )

    ssh_keys = [ssh_key_with_literals]

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
        assert key_content == expected_key_content

        # Verify it contains actual newlines, not literal \n
        assert "\\n" not in key_content
        assert "\n" in key_content


def test_setup_ssh_config_with_env_var(
    mock_context: Mock, sample_ssh_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test SSH config setup with dg.EnvVar containing SSH key."""
    env_var_name = "TEST_SSH_KEY"
    monkeypatch.setenv(env_var_name, sample_ssh_key)

    ssh_keys = [dg.EnvVar(env_var_name)]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        assert ssh_config_path is not None
        assert os.path.exists(ssh_config_path)

        # Read SSH config content
        with open(ssh_config_path, "r") as f:
            config_content = f.read()

        assert "Host *" in config_content
        assert "IdentityFile" in config_content
        assert "IdentitiesOnly yes" in config_content

        # Find the key file
        temp_dir = os.path.dirname(ssh_config_path)
        key_files = [f for f in os.listdir(temp_dir) if "_id_rsa_" in f]
        assert len(key_files) == 1
        key_file_path = os.path.join(temp_dir, key_files[0])

        # Verify key file content matches the environment variable value
        with open(key_file_path, "r") as f:
            key_content = f.read()
        expected_content = sample_ssh_key if sample_ssh_key.endswith("\n") else sample_ssh_key + "\n"
        assert key_content == expected_content

        # Verify file permissions
        key_stat = os.stat(key_file_path)
        assert key_stat.st_mode & 0o777 == 0o600


def test_setup_ssh_config_with_empty_env_var(mock_context: Mock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test SSH config setup with dg.EnvVar that has no value."""
    env_var_name = "EMPTY_SSH_KEY"
    # Don't set the environment variable, so it will be empty

    ssh_keys = [dg.EnvVar(env_var_name)]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        # Should still create SSH config file but with no key files
        assert ssh_config_path is not None
        assert os.path.exists(ssh_config_path)

        # SSH config should be empty (no Host entries since no valid keys)
        with open(ssh_config_path, "r") as f:
            config_content = f.read()
        assert config_content.strip() == ""

        # Should have no key files in temp directory
        temp_dir = os.path.dirname(ssh_config_path)
        key_files = [f for f in os.listdir(temp_dir) if "_id_rsa_" in f]
        assert len(key_files) == 0

    # Should still log SSH config setup message even if no keys were valid
    mock_context.log.info.assert_called_once_with("Setting up SSH configuration for Git authentication")


def test_setup_ssh_config_with_mixed_keys(
    mock_context: Mock, sample_ssh_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test SSH config setup with both string keys and dg.EnvVar keys."""
    env_var_name = "TEST_SSH_KEY_2"
    env_var_key = "env-var-ssh-key-content"
    monkeypatch.setenv(env_var_name, env_var_key)

    ssh_keys = [sample_ssh_key, dg.EnvVar(env_var_name)]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        assert ssh_config_path is not None
        assert os.path.exists(ssh_config_path)

        # Read SSH config content
        with open(ssh_config_path, "r") as f:
            config_content = f.read()

        # Should have 2 Host * entries (one per key)
        assert config_content.count("Host *") == 2
        assert config_content.count("IdentityFile") == 2

        # Verify temp directory has 2 key files
        temp_dir = os.path.dirname(ssh_config_path)
        key_files = sorted([f for f in os.listdir(temp_dir) if "_id_rsa_" in f])
        assert len(key_files) == 2

        # Verify first key file (string key)
        key_file_0 = [f for f in key_files if f.endswith("_id_rsa_0")][0]
        key_file_0_path = os.path.join(temp_dir, key_file_0)
        with open(key_file_0_path, "r") as f:
            key_content_0 = f.read()
        expected_content_0 = sample_ssh_key if sample_ssh_key.endswith("\n") else sample_ssh_key + "\n"
        assert key_content_0 == expected_content_0

        # Verify second key file (env var key)
        key_file_1 = [f for f in key_files if f.endswith("_id_rsa_1")][0]
        key_file_1_path = os.path.join(temp_dir, key_file_1)
        with open(key_file_1_path, "r") as f:
            key_content_1 = f.read()
        expected_content_1 = env_var_key if env_var_key.endswith("\n") else env_var_key + "\n"
        assert key_content_1 == expected_content_1


def test_setup_ssh_config_with_env_var_literal_newlines(mock_context: Mock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test SSH config setup with dg.EnvVar containing literal \\n characters."""
    env_var_name = "TEST_SSH_KEY_WITH_LITERALS"
    ssh_key_with_literals = (
        "-----BEGIN OPENSSH PRIVATE KEY-----\\nb3BlbnNzaC1rZXktdjE=\\n-----END OPENSSH PRIVATE KEY-----"
    )
    expected_key_content = (
        "-----BEGIN OPENSSH PRIVATE KEY-----\nb3BlbnNzaC1rZXktdjE=\n-----END OPENSSH PRIVATE KEY-----\n"
    )
    monkeypatch.setenv(env_var_name, ssh_key_with_literals)

    ssh_keys = [dg.EnvVar(env_var_name)]

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
        assert key_content == expected_key_content

        # Verify it contains actual newlines, not literal \n
        assert "\\n" not in key_content
        assert "\n" in key_content


def test_setup_ssh_config_with_multiple_env_vars(mock_context: Mock, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test SSH config setup with multiple dg.EnvVar keys."""
    env_var_1 = "TEST_SSH_KEY_1"
    env_var_2 = "TEST_SSH_KEY_2"
    key_content_1 = "ssh-key-content-1"
    key_content_2 = "ssh-key-content-2"

    monkeypatch.setenv(env_var_1, key_content_1)
    monkeypatch.setenv(env_var_2, key_content_2)

    ssh_keys = [dg.EnvVar(env_var_1), dg.EnvVar(env_var_2)]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        assert ssh_config_path is not None
        assert os.path.exists(ssh_config_path)

        # Read SSH config content
        with open(ssh_config_path, "r") as f:
            config_content = f.read()

        # Should have 2 Host * entries
        assert config_content.count("Host *") == 2
        assert config_content.count("IdentityFile") == 2

        # Verify temp directory has 2 key files
        temp_dir = os.path.dirname(ssh_config_path)
        key_files = sorted([f for f in os.listdir(temp_dir) if "_id_rsa_" in f])
        assert len(key_files) == 2

        # Verify both key files have correct content
        key_file_0 = [f for f in key_files if f.endswith("_id_rsa_0")][0]
        key_file_0_path = os.path.join(temp_dir, key_file_0)
        with open(key_file_0_path, "r") as f:
            content_0 = f.read()
        assert content_0 == key_content_1 + "\n"

        key_file_1 = [f for f in key_files if f.endswith("_id_rsa_1")][0]
        key_file_1_path = os.path.join(temp_dir, key_file_1)
        with open(key_file_1_path, "r") as f:
            content_1 = f.read()
        assert content_1 == key_content_2 + "\n"


def test_setup_ssh_config_with_some_empty_env_vars(
    mock_context: Mock, sample_ssh_key: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test SSH config setup with mix of valid and empty dg.EnvVar keys."""
    valid_env_var = "VALID_SSH_KEY"
    empty_env_var = "EMPTY_SSH_KEY"

    monkeypatch.setenv(valid_env_var, sample_ssh_key)
    # Don't set empty_env_var

    ssh_keys = [dg.EnvVar(valid_env_var), dg.EnvVar(empty_env_var)]

    with setup_ssh_config(mock_context, ssh_keys) as ssh_config_path:
        assert ssh_config_path is not None
        assert os.path.exists(ssh_config_path)

        # Should only create 1 key file (for the valid key)
        temp_dir = os.path.dirname(ssh_config_path)
        key_files = [f for f in os.listdir(temp_dir) if "_id_rsa_" in f]
        assert len(key_files) == 1

        # SSH config should only have 1 Host * entry
        with open(ssh_config_path, "r") as f:
            config_content = f.read()
        assert config_content.count("Host *") == 1
        assert config_content.count("IdentityFile") == 1

        # Verify the valid key file has correct content
        key_file_path = os.path.join(temp_dir, key_files[0])
        with open(key_file_path, "r") as f:
            key_content = f.read()
        expected_content = sample_ssh_key if sample_ssh_key.endswith("\n") else sample_ssh_key + "\n"
        assert key_content == expected_content
