import pytest
import types

from dagster_meltano_pipelines import core


class DummyField:
    pass


def test_setting_to_dagster_type_string() -> None:
    setting = {"kind": "string"}
    assert core.setting_to_dagster_type(setting) is str


def test_setting_to_dagster_type_int() -> None:
    setting = {"kind": "integer"}
    assert core.setting_to_dagster_type(setting) is int


def test_setting_to_dagster_type_enum() -> None:
    setting = {
        "kind": "options",
        "name": "color",
        "options": [{"value": "red", "label": "Red"}, {"value": "blue", "label": "Blue"}],
    }
    enum_type = core.setting_to_dagster_type(setting)
    assert hasattr(enum_type, "enum_values") or hasattr(enum_type, "_enum_values")


def test_plugin_setting_to_field_env(monkeypatch: pytest.MonkeyPatch) -> None:
    # Patch dg.EnvVar to return a dummy value
    class DummyEnvVar:
        def __init__(self, env: str) -> None:
            self.env = env

        def get_value(self) -> str:
            return "42"

    monkeypatch.setattr(
        core,
        "dg",
        types.SimpleNamespace(
            EnvVar=DummyEnvVar,
            Field=lambda *a, **k: (a, k),
            String="String",
            Int="Int",
            Float="Float",
            Bool="Bool",
            Enum=lambda **kwargs: type("DummyEnum", (), kwargs),
            EnumValue=lambda *a, **k: (a, k),
            Array=lambda **kwargs: type("DummyArray", (), kwargs),
            Any="Any",
        ),
    )
    monkeypatch.setattr(core, "setting_to_dagster_type", lambda s: "String")
    field = core.plugin_setting_to_field(setting={"name": "foo", "kind": "string"}, env_var="FOO_ENV")
    assert field[1]["default_value"] == "42"  # type: ignore[index]


def test_plugin_config_to_env_object() -> None:
    plugin_def = {"name": "tap-test", "settings": [{"name": "foo", "kind": "object"}]}
    config = {"foo": {"bar": 1}}
    env = core.plugin_config_to_env(plugin_def, config)
    assert "TAP_TEST_FOO" in env
    assert env["TAP_TEST_FOO"].startswith("{")


def test_plugin_config_to_env_string() -> None:
    plugin_def = {"name": "tap-test", "settings": [{"name": "foo", "kind": "string"}]}
    config = {"foo": "bar"}
    env = core.plugin_config_to_env(plugin_def, config)
    assert env["TAP_TEST_FOO"] == "bar"
