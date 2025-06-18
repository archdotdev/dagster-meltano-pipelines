import yaml
from pathlib import Path
from typing import Optional

import dagster as dg
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
        meltano_dir.mkdir(exist_ok=True, parents=True)

        # Create meltano.yml
        meltano_yml_path = meltano_dir / "meltano.yml"
        meltano_config = {
            "version": 1,
            "default_environment": "dev",
            "environments": [{"name": "dev"}, {"name": "staging"}, {"name": "prod"}],
            "plugins": {"extractors": [], "loaders": []},
            "venv": {"backend": "uv"},
        }

        with open(meltano_yml_path, "w") as f:
            yaml.dump(meltano_config, f, default_flow_style=False)

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
