import json
import sys
import typing as t
from dataclasses import dataclass
from functools import cached_property


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


def resolve_meltano_project(context: dg.ResolutionContext, model) -> MeltanoProject:
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

    def post_process(self, value):
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
        return dg.Array(inner_type=dg.Any())
    elif kind == "object":
        return MeltanoObjectType()

    return dg.String


def _first_or(list: t.List[t.Any], default: t.Any) -> t.Any:
    return list[0] if len(list) > 0 else default


def _plugin_to_dagster_resource(plugin: t.Dict[str, t.Any]) -> dg.ResourceDefinition:
    def _is_required(setting: t.Dict[str, t.Any]) -> bool:
        validation = setting["name"] in _first_or(plugin.get("settings_group_validation", []), [])
        default = setting.get("value")
        return default is None and validation

    config_schema = {
        setting["name"]: dg.Field(
            _setting_to_dagster_type(setting),
            default_value=setting.get("value", FIELD_NO_DEFAULT_PROVIDED),
            description=setting.get("description"),
            is_required=_is_required(setting),
        )
        for setting in plugin.get("settings", [])
    }

    @dg.resource(
        config_schema=config_schema,
        description=plugin.get("description"),
    )
    def meltano_plugin(context: dg.InitResourceContext) -> None:
        context.log.info("context.resource_config: %s", context.resource_config)
        return plugin

    return meltano_plugin


def _pipeline_to_dagster_asset(
    pipeline_id: str,
    extractor_id: str,
    loader_id: str,
    description: str | None = None,
):
    extractor_resource_key = f"{pipeline_id}_{extractor_id}"
    loader_resource_key = f"{pipeline_id}_{loader_id}"

    @dg.asset(
        name=pipeline_id,
        required_resource_keys={
            extractor_resource_key,
            loader_resource_key,
        },
        description=description or f"Move data from {extractor_id} to {loader_id}",
    )
    def meltano_job(context: dg.AssetExecutionContext) -> None:
        extractor_resource = getattr(context.resources, extractor_resource_key)
        loader_resource = getattr(context.resources, loader_resource_key)

        context.log.info("Running pipeline: %s", pipeline_id)
        context.log.info("Extractor: %s", extractor_resource)
        context.log.info("Loader: %s", loader_resource)

    return meltano_job


class MeltanoPipelineArgs(BaseModel):
    """Pipeline definition."""

    extractor: str
    loader: str
    description: str | None = None


@dataclass
class MeltanoPipelineComponent(dg.Component, dg.Resolvable):
    """A component that represents a Meltano project."""

    project: ResolvedMeltanoProject
    pipelines: t.Dict[str, MeltanoPipelineArgs]

    @cached_property
    def cli_resource(self):
        return MeltanoCliResource(self.project)

    @t.override
    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        assets = []
        resources = {}

        for pipeline_id, pipeline_args in self.pipelines.items():
            extractor_id = pipeline_args.extractor.replace("-", "_")
            loader_id = pipeline_args.loader.replace("-", "_")

            assets.append(
                _pipeline_to_dagster_asset(
                    pipeline_id,
                    extractor_id,
                    loader_id,
                    description=pipeline_args.description,
                )
            )

            resources[f"{pipeline_id}_{extractor_id}"] = _plugin_to_dagster_resource(
                self.project.plugins["extractors", pipeline_args.extractor],
            )

            resources[f"{pipeline_id}_{loader_id}"] = _plugin_to_dagster_resource(
                self.project.plugins["loaders", pipeline_args.loader],
            )

        return dg.Definitions(assets=assets, resources=resources)
