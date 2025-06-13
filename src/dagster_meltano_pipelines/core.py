import json
import typing as t
import dagster as dg
from dagster._config import config_type, config_schema, FIELD_NO_DEFAULT_PROVIDED


_all__ = [
    "plugin_config_to_env",
    "plugin_to_dagster_resource",
]


class MeltanoObjectType(config_type.ConfigType):
    def __init__(self) -> None:
        super().__init__(
            key="MeltanoObject",
            kind=config_type.ConfigTypeKind.ANY,
        )

    def post_process(self, value: t.Any) -> t.Any:
        return json.loads(value)


def setting_to_dagster_type(setting: t.Dict[str, t.Any]) -> config_schema.UserConfigSchema:
    kind = setting.get("kind", "string")
    if kind == "string":
        return dg.String
    elif kind == "integer":
        return dg.Int
    elif kind == "float":
        return dg.Float
    elif kind == "boolean":
        return dg.Bool
    elif kind == "options":
        return dg.Enum(
            name=setting.get("label", setting["name"]),
            enum_values=[
                dg.EnumValue(
                    option["value"],
                    description=option.get("label"),
                )
                for option in setting["options"]
            ],
        )
    elif kind == "array":
        return dg.Array(inner_type=dg.Any)
    elif kind == "object":
        return MeltanoObjectType()
    return dg.String


def first_or(list: t.List[t.Any], default: t.Any) -> t.Any:
    return list[0] if len(list) > 0 else default


def plugin_setting_to_field(
    *,
    setting: t.Dict[str, t.Any],
    env_var: str | None = None,
) -> dg.Field:
    if env_var:
        default_value = dg.EnvVar(env_var).get_value()
        if default_value and setting.get("kind") in ["object", "array"]:
            default_value = json.loads(default_value)
    else:
        default_value = FIELD_NO_DEFAULT_PROVIDED  # type: ignore[assignment]

    return dg.Field(
        setting_to_dagster_type(setting),
        default_value=default_value,
        description=setting.get("description"),
        is_required=False,
    )


def plugin_to_dagster_resource(
    plugin: t.Dict[str, t.Any],
    *,
    env_vars: t.Dict[str, str] | None = None,
) -> dg.ResourceDefinition:
    env_vars = env_vars or {}
    config_schema = {
        setting["name"]: plugin_setting_to_field(
            setting=setting,
            env_var=env_vars.get(setting["name"]),
        )
        for setting in plugin.get("settings", [])
    }

    @dg.resource(
        config_schema=config_schema,
        description=plugin.get("description"),
    )
    def meltano_plugin(context: dg.InitResourceContext) -> t.Tuple[t.Dict[str, t.Any], t.Dict[str, t.Any]]:
        if context.log:
            context.log.info("context.resource_config: %s", context.resource_config)
        return plugin, context.resource_config

    return meltano_plugin


def plugin_config_to_env(
    plugin_definition: t.Dict[str, t.Any],
    config: t.Dict[str, t.Any],
) -> t.Dict[str, str]:
    env = {}
    prefix = plugin_definition["name"].upper().replace("-", "_")
    settings_dict = {setting["name"]: setting for setting in plugin_definition.get("settings", [])}
    for key, value in config.items():
        suffix = key.upper()
        if key in settings_dict:
            kind = settings_dict[key].get("kind", "string")
            if kind in ["object", "array"]:
                value = json.dumps(value)
        env[f"{prefix}_{suffix}"] = str(value)
    return env
