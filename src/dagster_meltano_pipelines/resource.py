import subprocess
from dataclasses import dataclass
from pathlib import Path

from dagster._annotations import public
import dagster as dg

from dagster_meltano_pipelines.meltano_project import MeltanoProject


@dataclass
class MeltanoCliInvocation:
    process: subprocess.Popen[bytes]

    @classmethod
    def run(cls, *args: str, project_dir: str | Path) -> "MeltanoCliInvocation":
        process = subprocess.Popen(
            [*args],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=project_dir,
        )
        return cls(process)


class MeltanoCliResource(dg.ConfigurableResource):  # type: ignore[type-arg]
    """A resource that provides a Meltano CLI."""

    project: MeltanoProject
    meltano_executable: str = "meltano"

    @public
    def cli(self, *args: str) -> MeltanoCliInvocation:
        return MeltanoCliInvocation.run(
            self.meltano_executable,
            *args,
            project_dir=self.project.project_dir,
        )
