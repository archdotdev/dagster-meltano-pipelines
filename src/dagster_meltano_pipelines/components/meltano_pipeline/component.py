import os
import sys
import typing as t
import subprocess
from dataclasses import dataclass
from functools import cached_property

import dagster as dg
import orjson
from dagster.components.resolved.model import Resolver
from pydantic import BaseModel

from dagster_meltano_pipelines.meltano_project import MeltanoProject
from dagster_meltano_pipelines.resource import MeltanoCliResource
from dagster_meltano_pipelines.core import (
    plugin_config_to_env,
    plugin_to_dagster_resource,
)
from .scaffolder import MeltanoProjectScaffolder

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


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


def _process_meltano_logs(context: dg.AssetExecutionContext, lines: t.Iterable[bytes]) -> None:
    for line in lines:
        try:
            log_data = orjson.loads(line)
        except orjson.JSONDecodeError:
            continue

        level = log_data.pop("level")
        event = log_data.pop("event")
        context.log.log(level, event, extra={"meltano": log_data})


# Re-add MeltanoPlugin class for type annotations
class MeltanoPlugin(BaseModel):
    """Base class for Meltano plugins."""

    name: str
    env_vars: t.Optional[t.Dict[str, str]] = None

    @property
    def id(self) -> str:
        return self.name.replace("-", "_")


def pipeline_to_dagster_asset(
    pipeline_id: str,
    *,
    project: MeltanoProject,
    extractor: MeltanoPlugin,
    loader: MeltanoPlugin,
    description: t.Optional[str] = None,
    tags: t.Optional[t.Dict[str, str]] = None,
) -> dg.AssetsDefinition:
    extractor_plugin = project.plugins["extractors", extractor.name]
    loader_plugin = project.plugins["loaders", loader.name]

    @dg.asset(
        name=pipeline_id,
        required_resource_keys={
            extractor.id,
            loader.id,
        },
        description=description or f"Move data from {extractor.id} to {loader.id}",
        metadata={
            "extractor": extractor_plugin,
            "loader": loader_plugin,
        },
        tags=tags,
    )
    def meltano_job(context: dg.AssetExecutionContext) -> None:
        extractor_config = getattr(context.resources, extractor.name)
        loader_config = getattr(context.resources, loader.name)

        context.log.info("Running pipeline: %s", pipeline_id)

        env: t.Dict[str, str] = {**os.environ}
        env |= plugin_config_to_env(extractor_plugin, extractor_config)
        env |= plugin_config_to_env(loader_plugin, loader_config)

        context.log.info("Env: %s", env)

        process = subprocess.Popen(
            [
                "meltano",
                "run",
                f"--run-id={context.run_id}",
                extractor.name,
                loader.name,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=project.project_dir,
            env=env,
            text=False,
            bufsize=1,
        )

        # Stream logs in real time
        if process.stdout is not None:
            for line in iter(process.stdout.readline, b""):
                try:
                    log_data = orjson.loads(line)
                except orjson.JSONDecodeError:
                    # If it's not valid JSON, log as raw text
                    context.log.info(line.decode("utf-8").strip())
                else:
                    level = log_data.pop("level")
                    event = log_data.pop("event")
                    context.log.log(level, event, extra={"meltano": log_data})

            # Wait for process to complete
            exit_code = process.wait()

            if exit_code != 0:
                raise RuntimeError(f"Meltano job failed with exit code {exit_code}")

    return meltano_job


class MeltanoPipelineArgs(BaseModel):
    """Pipeline definition."""

    extractor: MeltanoPlugin
    loader: MeltanoPlugin
    description: t.Optional[str] = None
    tags: t.Optional[t.Dict[str, str]] = None
    env_vars: t.Optional[t.Dict[str, str]] = None


@dg.scaffold_with(MeltanoProjectScaffolder)
@dataclass
class MeltanoPipelineComponent(dg.Component, dg.Resolvable):
    """A component that represents a Meltano pipeline.

    Use `dg scaffold dagster_meltano_pipelines.MeltanoPipelineComponent {component_path}` to get started.
    """

    project: ResolvedMeltanoProject
    pipelines: t.Dict[str, MeltanoPipelineArgs]

    @cached_property
    def cli_resource(self) -> MeltanoCliResource:
        return MeltanoCliResource(project=self.project)

    @override
    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        assets = []
        resources = {}

        for (_, plugin_name), plugin in self.project.plugins.items():
            resources[plugin_name.replace("-", "_")] = plugin_to_dagster_resource(plugin)

        for pipeline_id, pipeline_args in self.pipelines.items():
            assets.append(
                pipeline_to_dagster_asset(
                    pipeline_id,
                    project=self.project,
                    extractor=pipeline_args.extractor,
                    loader=pipeline_args.loader,
                    description=pipeline_args.description,
                    tags=pipeline_args.tags,
                )
            )

        return dg.Definitions(assets=assets, resources=resources)
