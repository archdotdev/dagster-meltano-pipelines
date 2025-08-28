import contextlib
import json
import os
import subprocess
import sys
import tempfile
import typing as t
from contextlib import contextmanager
from dataclasses import dataclass
from importlib.metadata import version

import dagster as dg
import orjson
from dagster.components.resolved.model import Resolver
from pydantic import BaseModel, Field

from dagster_meltano_pipelines.project import MeltanoProject
from dagster_meltano_pipelines.resources import Extractor, Loader, MeltanoConfig

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


def get_all_ssh_keys(pipeline: "MeltanoPipeline") -> t.List[str]:
    """Collect SSH keys from all sources with deprecation warning.

    Args:
        pipeline: The Meltano pipeline configuration

    Returns:
        List of SSH private key contents
    """
    ssh_keys = []

    # Collect keys from plugins (new approach)
    if pipeline.extractor.git_ssh_private_key:
        ssh_keys.append(pipeline.extractor.git_ssh_private_key)
    if pipeline.loader.git_ssh_private_key:
        ssh_keys.append(pipeline.loader.git_ssh_private_key)

    # Handle deprecated pipeline-level keys
    if pipeline.git_ssh_private_keys:
        import warnings

        warnings.warn(
            "Pipeline-level git_ssh_private_keys is deprecated. "
            "Configure git_ssh_private_key on individual extractor and loader plugins instead.",
            DeprecationWarning,
            stacklevel=3,
        )
        ssh_keys.extend(pipeline.git_ssh_private_keys)

    return ssh_keys


@contextmanager
def setup_ssh_config(
    context: dg.AssetExecutionContext,
    ssh_private_keys: t.List[str],
) -> t.Generator[t.Optional[str], None, None]:
    """Create temporary SSH config and key files for Git authentication.

    Yields:
        SSH config file path
    """
    if not ssh_private_keys:
        yield None
        return

    context.log.info("Setting up SSH configuration for Git authentication")

    # Use context managers to ensure files stay open and accessible
    with tempfile.TemporaryDirectory(prefix="meltano_ssh_") as temp_dir:
        # Use ExitStack to manage multiple key file contexts properly
        with contextlib.ExitStack() as stack:
            key_files = []

            # Create and enter context for each key file
            for i, key_content in enumerate(ssh_private_keys):
                # Replace literal \n with actual newlines
                key_content = key_content.replace("\\n", "\n")
                if not key_content.endswith("\n"):
                    key_content += "\n"

                key_file = stack.enter_context(
                    tempfile.NamedTemporaryFile(mode="w", suffix=f"_id_rsa_{i}", dir=temp_dir, delete=False)
                )
                key_file.write(key_content)
                key_file.flush()
                os.chmod(key_file.name, 0o600)
                key_files.append(key_file.name)

            # Create SSH config file
            ssh_config_content = []
            for key_file_path in key_files:
                ssh_config_content.extend(
                    [
                        "Host *",
                        f"    IdentityFile {key_file_path}",
                        "    IdentitiesOnly yes",
                        "    StrictHostKeyChecking no",
                        "    UserKnownHostsFile /dev/null",
                        "",
                    ]
                )

            ssh_config_file = tempfile.NamedTemporaryFile(mode="w", suffix="_ssh_config", dir=temp_dir, delete=False)
            ssh_config_file.write("\n".join(ssh_config_content))
            ssh_config_file.close()

            yield ssh_config_file.name


def build_pipeline_env(
    pipeline: "MeltanoPipeline",
    project: MeltanoProject,
    ssh_config_path: t.Optional[str] = None,
    base_env: t.Optional[t.Dict[str, str]] = None,
    flags: t.Optional["MeltanoRunConfig"] = None,
) -> t.Dict[str, str]:
    """Build environment variables for the Meltano pipeline.

    Args:
        pipeline: The Meltano pipeline configuration
        project: The Meltano project instance
        ssh_config_path: Path to SSH config file, if any
        base_env: Base environment variables (defaults to os.environ)

    Returns:
        Dictionary of environment variables for the pipeline
    """
    if base_env is None:
        base_env = dict(os.environ)
    else:
        base_env = dict(base_env)

    # Prevent MELTANO_PROJECT_ROOT from interfering with configured project location
    base_env.pop("MELTANO_PROJECT_ROOT", None)

    # Add meltano config if present
    if pipeline.meltano_config:
        base_env.update(pipeline.meltano_config.as_env())

    # Build final environment with all pipeline-specific variables
    env = {
        **base_env,
        **pipeline.extractor.as_env(),
        **pipeline.loader.as_env(),
        **pipeline.env,
    }

    # Set JSON log format as default if not already configured
    if "MELTANO_CLI_LOG_FORMAT" not in env:
        env["MELTANO_CLI_LOG_FORMAT"] = "json"

    # Add SSH config if provided
    if ssh_config_path:
        env["GIT_SSH_COMMAND"] = f"ssh -F {ssh_config_path}"

    # Add select_filter if provided in runtime config
    if flags and flags.select_filter is not None:
        extractor_name = pipeline.extractor.name.upper().replace("-", "_")
        env[f"{extractor_name}__SELECT_FILTER"] = json.dumps(flags.select_filter)

    return env


def _run_meltano_pipeline(
    context: dg.AssetExecutionContext,
    pipeline: "MeltanoPipeline",
    project: MeltanoProject,
    env: t.Dict[str, str],
    *,
    flags: "MeltanoRunConfig",
) -> None:
    """Execute the Meltano pipeline."""
    command = flags.get_command(run_id=context.run_id, state_suffix=pipeline.state_suffix)

    context.add_asset_metadata(
        {
            "meltano_version": version("meltano"),
            "component_version": version("dagster-meltano-pipelines"),
        }
    )

    process = subprocess.Popen(
        [
            *command,
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
                context.log.log(level, event)
                if "Extractor failed" in event or "Loader failed" in event or "Mappers failed" in event:
                    context.add_asset_metadata(
                        {
                            "code": log_data.pop("code", None),
                            "message": log_data.pop("message", None),
                            "exception": log_data.pop("exception", []),
                        }
                    )

                if "Run completed" in event:
                    context.add_asset_metadata(
                        {
                            "duration_seconds": log_data.pop("duration_seconds", None),
                        }
                    )

        # Wait for process to complete
        exit_code = process.wait()

        if exit_code != 0:
            raise RuntimeError(f"Meltano job failed with exit code {exit_code}")


def pipeline_to_dagster_asset(
    pipeline: "MeltanoPipeline",
    *,
    project: MeltanoProject,
    props: t.Optional["DagsterAssetProps"] = None,
) -> dg.AssetsDefinition:
    extractor_definition = project.plugins["extractors", pipeline.extractor.name]
    loader_definition = project.plugins["loaders", pipeline.loader.name]
    props = props or DagsterAssetProps()
    tags: t.Dict[str, str] = {
        "extractor": pipeline.extractor.name,
        "loader": pipeline.loader.name,
    }
    if props.tags:
        tags.update(props.tags)
    if pipeline.tags:
        tags.update(pipeline.tags)

    @dg.asset(
        name=pipeline.id,
        description=pipeline.description or f"{pipeline.extractor.name} → {pipeline.loader.name}",
        tags=tags,
        kinds={"Meltano"},
        metadata={
            "extractor": extractor_definition,
            "loader": loader_definition,
        },
        key_prefix=props.key_prefix,
    )
    def meltano_job(context: dg.AssetExecutionContext, config: MeltanoRunConfig) -> None:
        context.log.info("Running pipeline: %s", pipeline.id)

        # Log warning if MELTANO_PROJECT_ROOT was removed
        if "MELTANO_PROJECT_ROOT" in os.environ:
            context.log.warning(
                "Removing MELTANO_PROJECT_ROOT environment variable (value: %s) to prevent "
                "interference with configured project directory: %s",
                os.environ["MELTANO_PROJECT_ROOT"],
                project.project_dir,
            )

        with setup_ssh_config(context, get_all_ssh_keys(pipeline)) as ssh_config_path:
            env = build_pipeline_env(pipeline, project, ssh_config_path, flags=config)
            _run_meltano_pipeline(context, pipeline, project, env, flags=config)

    return meltano_job


class MeltanoRunConfig(dg.Config):
    """Flags to pass to the Meltano pipeline."""

    #: Whether to execute the pipeline ignoring any existing state
    full_refresh: bool = False

    #: Whether to refresh the catalog before executing the pipeline
    refresh_catalog: bool = False

    #: How to handle state updates
    state_strategy: t.Literal["auto", "merge", "overwrite"] = "auto"

    #: Log level for Meltano CLI
    log_level: t.Optional[str] = None

    #: Stream selection filter
    select_filter: t.Optional[t.List[str]] = None

    def get_command(self, *, run_id: str, state_suffix: t.Optional[str] = None) -> t.List[str]:
        """Get the command to run Meltano with the flags.

        Args:
            run_id: The run ID to pass to Meltano
            state_suffix: Optional state suffix for the pipeline

        Returns:
            List of command parts for running Meltano
        """
        command = ["meltano"]

        if self.log_level:
            command.append(f"--log-level={self.log_level}")

        command.extend(
            [
                "run",
                f"--run-id={run_id}",
            ]
        )

        if state_suffix:
            command.append(f"--state-id-suffix={state_suffix}")

        if self.full_refresh:
            command.append("--full-refresh")

        if self.refresh_catalog:
            command.append("--refresh-catalog")

        if self.state_strategy != "auto":
            command.append(f"--state-strategy={self.state_strategy}")

        return command


class MeltanoPipeline(BaseModel):
    """Pipeline definition."""

    id: str
    extractor: Extractor
    loader: Loader
    description: t.Optional[str] = None
    tags: t.Optional[t.Dict[str, str]] = None
    meltano_config: t.Optional[MeltanoConfig] = Field(None, description="Meltano configuration")
    env: t.Dict[str, str] = Field(
        default_factory=dict,
        description="Environment variables to pass to the Meltano pipeline",
    )
    git_ssh_private_keys: t.List[str] = Field(
        default_factory=list,
        description="(DEPRECATED) List of SSH private key contents for Git authentication. "
        "Use git_ssh_private_key on individual extractor and loader plugins instead.",
    )

    state_suffix: t.Optional[str] = Field(None, description="Suffix to add to the state backend environment variables")


class DagsterAssetProps(BaseModel):
    """Properties that apply to all assets generated by the component."""

    key_prefix: t.Optional[t.Union[str, t.Sequence[str]]] = Field(
        default=None,
        description="Key prefix to use for the assets generated by the component",
    )
    tags: t.Optional[t.Dict[str, str]] = Field(
        default=None,
        description="Tags to apply to the assets generated by the component",
    )


@dg.scaffold_with(MeltanoProjectScaffolder)
@dataclass
class MeltanoPipelineComponent(dg.Component, dg.Resolvable):
    """A component that represents a Meltano pipeline.

    Use `dg scaffold dagster_meltano_pipelines.MeltanoPipelineComponent {component_path}` to get started.
    """

    project: ResolvedMeltanoProject
    pipelines: t.List[MeltanoPipeline]
    asset_props: t.Optional[DagsterAssetProps] = None

    @override
    def build_defs(self, context: dg.ComponentLoadContext) -> dg.Definitions:
        assets = []
        seen_ids = set()

        for pipeline in self.pipelines:
            if pipeline.id in seen_ids:
                msg = f"Pipeline ID {pipeline.id} is not unique"
                raise ValueError(msg)

            seen_ids.add(pipeline.id)

            assets.append(
                pipeline_to_dagster_asset(
                    pipeline,
                    project=self.project,
                    props=self.asset_props,
                )
            )

        return dg.Definitions(assets=assets)
