from pathlib import Path
from typing import Optional

import dagster as dg
import yaml
from meltano.cli import cli as meltano_cli  # type: ignore[attr-defined]
from pydantic import BaseModel


class MeltanoScaffolderParams(BaseModel):
    project_path: Optional[Path] = None


class MeltanoProjectScaffolder(dg.Scaffolder[MeltanoScaffolderParams]):
    @classmethod
    def get_scaffold_params(cls) -> type[MeltanoScaffolderParams]:
        return MeltanoScaffolderParams

    def scaffold(self, request: dg.ScaffoldRequest[MeltanoScaffolderParams]) -> None:
        # Support custom project path similar to dbt scaffolder
        project_path = "project"  # default
        if request.params.project_path:
            if request.params.project_path.is_absolute():
                # For absolute paths, use as-is
                project_path = str(request.params.project_path)
            else:
                # For relative paths, prefix with {{ project_root }}
                project_path = f"{{{{ project_root }}}}/{request.params.project_path}"

        dg.scaffold_component(
            request,
            {
                "project": project_path,
                "pipelines": {},
            },
        )

        # Create the meltano project directory
        if request.params.project_path:
            meltano_dir = request.params.project_path
        else:
            meltano_dir = request.target_path / "project"

        # Invoke the meltano CLI to scaffold the project
        meltano_cli(
            [
                "init",
                meltano_dir.as_posix(),
            ],
        )

        # Create logging.yaml
        logging_yaml_path = meltano_dir / "logging.yaml"
        logging_config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {"json": {"()": "meltano.core.logging.json_formatter"}},
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "level": "DEBUG",
                    "formatter": "json",
                    "stream": "ext://sys.stderr",
                }
            },
            "loggers": {
                "meltano.core.block.extract_load": {"level": "INFO"},
                "meltano.core.plugin.singer.catalog": {"level": "INFO"},
                "smart_open": {"level": "INFO"},
                "botocore": {"level": "INFO"},
            },
            "root": {"level": "DEBUG", "handlers": ["console"]},
        }

        with open(logging_yaml_path, "w") as f:
            yaml.dump(logging_config, f, default_flow_style=False)
