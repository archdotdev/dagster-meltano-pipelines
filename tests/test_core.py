import typing as t

import dagster as dg
import pytest

from dagster_meltano_pipelines import core


@pytest.fixture
def plugin_definition() -> dict[str, t.Any]:
    return {
        "name": "tap-test",
        "settings": [
            {"name": "a_string", "kind": "string"},
            {"name": "an_object", "kind": "object"},
        ],
    }


def test_plugin_config_to_env_object(plugin_definition: dict[str, t.Any]) -> None:
    config = {"an_object": {"bar": 1}}
    assert core.plugin_config_to_env(plugin_definition, config) == {"TAP_TEST_AN_OBJECT": '{"bar": 1}'}


def test_plugin_config_to_env_string(plugin_definition: dict[str, t.Any]) -> None:
    config = {"a_string": "bar"}
    assert core.plugin_config_to_env(plugin_definition, config) == {"TAP_TEST_A_STRING": "bar"}


def test_plugin_config_to_env_dagster_env_var(
    plugin_definition: dict[str, t.Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FOO", "bar")
    config = {"a_string": dg.EnvVar("FOO")}
    assert core.plugin_config_to_env(plugin_definition, config) == {"TAP_TEST_A_STRING": "bar"}
