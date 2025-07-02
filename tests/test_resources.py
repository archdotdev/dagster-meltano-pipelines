import dagster as dg
import pytest

from dagster_meltano_pipelines.resources import Extractor, ExtractorConfig


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
