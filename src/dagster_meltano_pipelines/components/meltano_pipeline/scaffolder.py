import os
import yaml
from dagster.components.component.component_scaffolder import Scaffolder  # type: ignore[attr-defined]
from dagster.components.component_scaffolding import scaffold_component
from dagster.components.scaffold.scaffold import ScaffoldRequest


class MeltanoProjectScaffolder(Scaffolder):
    def scaffold(self, request: ScaffoldRequest) -> None:
        # Support custom project path similar to dbt scaffolder
        project_path = "project"  # default
        if hasattr(request.params, "project_path") and request.params.project_path:
            project_root = request.target_path.parent
            rel_path = os.path.relpath(request.params.project_path, start=project_root)
            project_path = f"{{{{ project_root }}}}/{rel_path}"

        scaffold_component(
            request,
            {
                "project": project_path,
                "pipelines": {},
            },
        )

        # Create the meltano project directory
        if hasattr(request.params, "project_path") and request.params.project_path:
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
