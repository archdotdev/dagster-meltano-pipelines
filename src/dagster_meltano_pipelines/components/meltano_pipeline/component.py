import os
import subprocess
import sys
import typing as t
from dataclasses import dataclass

import dagster as dg
import orjson
from dagster.components.resolved.model import Resolver
from pydantic import BaseModel, Field

from dagster_meltano_pipelines.core import plugin_config_to_env
from dagster_meltano_pipelines.project import MeltanoProject
from dagster_meltano_pipelines.resources import Extractor, Loader

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


def resolve_meltano_project(
    context: dg.ResolutionContext,
    model: BaseModel,
) -> MeltanoProject:
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


def pipeline_to_dagster_asset(
    pipeline_id: str,
    *,
    pipeline: "MeltanoPipeline",
    project: MeltanoProject,
    description: t.Optional[str] = None,
    tags: t.Optional[t.Dict[str, str]] = None,
) -> dg.AssetsDefinition:
    extractor_definition = project.plugins["extractors", pipeline.extractor.name]
    loader_definition = project.plugins["loaders", pipeline.loader.name]

    @dg.asset(
        name=pipeline_id,
        description=description or f"{pipeline.extractor.name} → {pipeline.loader.name}",
        tags=tags,
        kinds={"Meltano"},
    )
    def meltano_job(context: dg.AssetExecutionContext) -> None:
        context.log.info("Running pipeline: %s", pipeline_id)
        env: t.Dict[str, str] = {
            **os.environ,
            **pipeline.env,
        }

        if pipeline.extractor.config:
            env |= plugin_config_to_env(extractor_definition, pipeline.extractor.config.model_dump())

        if pipeline.loader.config:
            env |= plugin_config_to_env(loader_definition, pipeline.loader.config.model_dump())

        # TODO: Remove this
        context.log.info("Env: %s", env)

        process = subprocess.Popen(
            [
                "meltano",
                "run",
                f"--run-id={context.run_id}",
                pipeline.extractor.name,
                pipeline.loader.name,
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


class MeltanoPipeline(BaseModel):
    """Pipeline definition."""

    extractor: Extractor
    loader: Loader
    description: t.Optional[str] = None
    tags: t.Optional[t.Dict[str, str]] = None
    env: t.Dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables to pass to the Meltano pipeline",
    )


@dg.scaffold_with(MeltanoProjectScaffolder)
@dataclass
class MeltanoPipelineComponent(dg.Component, dg.Resolvable):
    """A component that represents a Meltano pipeline.

    Use `dg scaffold dagster_meltano_pipelines.MeltanoPipelineComponent {component_path}` to get started.
    """

    project: ResolvedMeltanoProject
    pipelines: t.Dict[str, MeltanoPipeline]

    @override
    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        assets = []

        for pipeline_id, pipeline_args in self.pipelines.items():
            assets.append(
                pipeline_to_dagster_asset(
                    pipeline_id,
                    project=self.project,
                    pipeline=pipeline_args,
                    description=pipeline_args.description,
                    tags=pipeline_args.tags,
                )
            )

        return dg.Definitions(assets=assets)
