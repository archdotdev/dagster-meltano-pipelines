from __future__ import annotations

import json
import typing as t
import dagster as dg


_all__ = [
    "plugin_config_to_env",
]


def plugin_config_to_env(
    plugin_definition: t.Dict[str, t.Any],
    config: t.Dict[str, t.Any],
) -> t.Dict[str, str]:
    env = {}
    prefix = plugin_definition["name"].upper().replace("-", "_")
    settings_dict = {setting["name"]: setting for setting in plugin_definition.get("settings", [])}

    for key, value in config.items():
        suffix = key.upper()

        # Retrieve the value from the environment variable
        if isinstance(value, dg.EnvVar):
            value = value.get_value()

        # Convert array and object values to JSON strings (only if they are not coming from an environment variable)
        elif key in settings_dict:
            kind = settings_dict[key].get("kind", "string")
            if kind in ["object", "array"]:
                value = json.dumps(value)

        if value is not None:
            env[f"{prefix}_{suffix}"] = str(value)

    return env
