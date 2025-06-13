import json
import os
import sys
import typing as t
import subprocess
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path

import dagster as dg
from dagster.components.resolved.model import Resolver
from pydantic import BaseModel

from dagster._config import config_type, config_schema, FIELD_NO_DEFAULT_PROVIDED
from dagster_meltano_pipelines.meltano_project import MeltanoProject
from dagster_meltano_pipelines.resource import MeltanoCliResource

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias


@dataclass
class MeltanoProjectArgs(dg.Resolvable):
    """Aligns with MeltanoProject.__new__."""

    project_dir: str


def resolve_meltano_project(context: dg.ResolutionContext, model: BaseModel) -> MeltanoProject:
    if isinstance(model, str):
        return MeltanoProject(
            context.resolve_source_relative_path(
                context.resolve_value(model, as_type=str),
            )
        )

    args = MeltanoProjectArgs.resolve_from_model(context, model)

    return MeltanoProject(
        project_dir=context.resolve_source_relative_path(args.project_dir),
    )


ResolvedMeltanoProject: TypeAlias = t.Annotated[
    MeltanoProject,
    Resolver(
        resolve_meltano_project,
        model_field_type=t.Union[str, MeltanoProjectArgs.model()],
    ),
]


class MeltanoObjectType(config_type.ConfigType):
    def __init__(self) -> None:
        super().__init__(
            key="MeltanoObject",
            kind=config_type.ConfigTypeKind.ANY,
        )

    def post_process(self, value: t.Any) -> t.Any:
        return json.loads(value)


def _setting_to_dagster_type(setting: t.Dict[str, t.Any]) -> config_schema.UserConfigSchema:
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


def _first_or(list: t.List[t.Any], default: t.Any) -> t.Any:
    return list[0] if len(list) > 0 else default


def _plugin_setting_to_field(
    *,
    plugin_config: t.Dict[str, t.Any],
    setting: t.Dict[str, t.Any],
    group_validation: t.List[t.List[str]],
    env_var: str | None = None,
) -> dg.Field:
    if env_var:
        default_value = dg.EnvVar(env_var).get_value()
        if setting.get("kind") in ["object", "array"]:
            default_value = json.loads(default_value)
    else:
        default_value = FIELD_NO_DEFAULT_PROVIDED

    return dg.Field(
        _setting_to_dagster_type(setting),
        default_value=default_value,
        description=setting.get("description"),
        is_required=False,
    )


def _plugin_to_dagster_resource(
    plugin: t.Dict[str, t.Any],
    *,
    env_vars: t.Dict[str, str] | None = None,
) -> dg.ResourceDefinition:
    env_vars = env_vars or {}
    config_schema = {
        setting["name"]: _plugin_setting_to_field(
            plugin_config=plugin.get("config", {}),
            setting=setting,
            group_validation=plugin.get("settings_group_validation", []),
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


class MeltanoPlugin(BaseModel):
    """Base class for Meltano plugins."""

    name: str
    env_vars: t.Dict[str, str] | None = None

    @property
    def id(self) -> str:
        return self.name.replace("-", "_")


def _plugin_config_to_env(
    plugin_definition: t.Dict[str, t.Any],
    config: t.Dict[str, t.Any],
) -> t.Dict[str, t.Any]:
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


def _pipeline_to_dagster_asset(
    pipeline_id: str,
    *,
    project_dir: Path,
    extractor: MeltanoPlugin,
    loader: MeltanoPlugin,
    description: str | None = None,
    tags: t.Dict[str, str] | None = None,
) -> dg.AssetsDefinition:
    extractor_resource_key = f"{pipeline_id}_{extractor.id}"
    loader_resource_key = f"{pipeline_id}_{loader.id}"

    @dg.asset(
        name=pipeline_id,
        required_resource_keys={
            extractor_resource_key,
            loader_resource_key,
        },
        description=description or f"Move data from {extractor.id} to {loader.id}",
        tags=tags,
    )
    def meltano_job(context: dg.AssetExecutionContext) -> None:
        extractor_resource, extractor_config = getattr(context.resources, extractor_resource_key)
        loader_resource, loader_config = getattr(context.resources, loader_resource_key)
        env = {}

        context.log.info("Running pipeline: %s", pipeline_id)
        context.log.info("Extractor: %s", extractor_resource)
        context.log.info("Loader: %s", loader_resource)

        env |= os.environ
        env |= _plugin_config_to_env(extractor_resource, extractor_config)
        env |= _plugin_config_to_env(loader_resource, loader_config)

        context.log.info("Env: %s", env)

        process = subprocess.Popen(
            [
                "meltano",
                "--log-format=json",
                "run",
                extractor.name,
                loader.name,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=project_dir,
            env=env,
        )
        if exit_code := process.wait():
            if process.stdout is not None:
                context.log.error(process.stdout.read().decode("utf-8"))

            raise RuntimeError(f"Meltano job failed with exit code {exit_code}")

        if process.stdout is not None:
            context.log.info(process.stdout.read().decode("utf-8"))

        # if not process.stderr:
        #     return

        # for line in process.stderr:
        #     context.log.error(line.decode("utf-8"))

    return meltano_job


class MeltanoPipelineArgs(BaseModel):
    """Pipeline definition."""

    extractor: MeltanoPlugin
    loader: MeltanoPlugin
    description: str | None = None
    env_vars: t.Dict[str, str] | None = None


@dataclass
class MeltanoPipelineComponent(dg.Component, dg.Resolvable):
    """A component that represents a Meltano pipeline."""

    project: ResolvedMeltanoProject
    pipelines: t.Dict[str, MeltanoPipelineArgs]
    tags: t.Dict[str, str] | None = None

    @cached_property
    def cli_resource(self) -> MeltanoCliResource:
        return MeltanoCliResource(project=self.project)

    @t.override
    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        assets = []
        resources = {}

        for pipeline_id, pipeline_args in self.pipelines.items():
            assets.append(
                _pipeline_to_dagster_asset(
                    pipeline_id,
                    project_dir=self.project.project_dir,
                    extractor=pipeline_args.extractor,
                    loader=pipeline_args.loader,
                    description=pipeline_args.description,
                    tags=self.tags,
                )
            )

            resources[f"{pipeline_id}_{pipeline_args.extractor.id}"] = _plugin_to_dagster_resource(
                self.project.plugins["extractors", pipeline_args.extractor.name],
                env_vars=pipeline_args.extractor.env_vars,
            )

            resources[f"{pipeline_id}_{pipeline_args.loader.id}"] = _plugin_to_dagster_resource(
                self.project.plugins["loaders", pipeline_args.loader.name],
                env_vars=pipeline_args.loader.env_vars,
            )

        return dg.Definitions(assets=assets, resources=resources)
