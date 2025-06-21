import json
import typing as t
from pathlib import Path

import yaml
from dagster._record import IHaveNew, record_custom

from dagster_meltano_pipelines.errors import DagsterMeltanoProjectNotFoundError


@record_custom
class MeltanoProject(IHaveNew):
    """A component that represents a Meltano project."""

    project_dir: Path
    plugins: dict[tuple[str, str], dict[str, t.Any]]

    def __new__(
        cls,
        project_dir: t.Union[Path, str],
    ) -> "MeltanoProject":
        project_dir = Path(project_dir)
        if not project_dir.exists():
            msg = f"project_dir {project_dir} does not exist."
            raise DagsterMeltanoProjectNotFoundError(msg)

        with open(project_dir.joinpath("meltano.yml")) as file:
            meltano_yml = yaml.safe_load(file)

        plugin_defs: dict[tuple[str, str], dict[str, t.Any]] = {}
        for plugin_type, plugins in meltano_yml.get("plugins", {}).items():
            for plugin in plugins:
                if "inherit_from" in plugin:
                    continue
                if plugin.get("namespace"):
                    # This is a custom plugin, the definition is inlined
                    plugin_defs[plugin_type, plugin["name"]] = plugin
                else:
                    # Read from $project_dir/plugins/$plugin_type/$plugin_name--$plugin_variant.lock
                    plugin_lock_file = project_dir.joinpath(
                        "plugins", plugin_type, f"{plugin['name']}--{plugin['variant']}.lock"
                    )
                    with open(plugin_lock_file) as file:
                        plugin_def = json.load(file)
                    plugin_defs[plugin_type, plugin["name"]] = {**plugin_def, **plugin}

        return super().__new__(
            cls,
            project_dir=project_dir,
            plugins=plugin_defs,
        )
