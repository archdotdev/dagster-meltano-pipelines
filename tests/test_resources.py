import dagster as dg
import pytest

from dagster_meltano_pipelines.resources import (
    CLIConfig,
    ELTConfig,
    Extractor,
    ExtractorConfig,
    MeltanoConfig,
    StateBackendConfig,
    VenvConfig,
)


@pytest.fixture
def extractor() -> Extractor:
    return Extractor(
        name="tap-test",
        config=ExtractorConfig(  # type: ignore[call-arg]
            api_key="s3c3r3t",
            mapping={
                "foo": "bar",
                "baz": "qux",
            },
            sequence=[1, 2, 3],
            from_dagster=dg.EnvVar("FROM_DAGSTER"),
            _catalog="catalog.json",
            _select=["foo.*", "baz.*"],
        ),
    )


@pytest.fixture
def meltano_config() -> MeltanoConfig:
    return MeltanoConfig(
        state_backend=StateBackendConfig(
            uri="mybackend://some/path",
            mybackend={  # type: ignore[call-arg]
                "api_url": "https://api.mybackend.com",
                "secret": dg.EnvVar("MY_BACKEND_SECRET"),
            },
        ),
        venv=VenvConfig(backend="virtualenv"),
        cli=CLIConfig(log_level="debug", log_format="json"),
        elt=ELTConfig(buffer_size=104_857_600),
    )


def test_plugin_as_env(extractor: Extractor, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FROM_DAGSTER", "from_dagster")
    assert extractor.as_env() == {
        "TAP_TEST_API_KEY": "s3c3r3t",
        "TAP_TEST_MAPPING": '{"foo": "bar", "baz": "qux"}',
        "TAP_TEST_SEQUENCE": "[1, 2, 3]",
        "TAP_TEST_FROM_DAGSTER": "from_dagster",
        "TAP_TEST__CATALOG": "catalog.json",
        "TAP_TEST__SELECT": '["foo.*", "baz.*"]',
    }


def test_meltano_config_as_env(meltano_config: MeltanoConfig, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_BACKEND_SECRET", "s3c3r3t")
    assert meltano_config.as_env() == {
        "MELTANO_STATE_BACKEND_URI": "mybackend://some/path",
        "MELTANO_STATE_BACKEND_MYBACKEND_API_URL": "https://api.mybackend.com",
        "MELTANO_STATE_BACKEND_MYBACKEND_SECRET": "s3c3r3t",
        "MELTANO_VENV_BACKEND": "virtualenv",
        "MELTANO_CLI_LOG_LEVEL": "debug",
        "MELTANO_CLI_LOG_FORMAT": "json",
        "MELTANO_ELT_BUFFER_SIZE": "104857600",
    }
