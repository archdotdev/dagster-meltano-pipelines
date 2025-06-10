import subprocess
from dataclasses import dataclass

from dagster._annotations import public
import dagster as dg


@dataclass
class MeltanoCliInvocation:
    process: subprocess.Popen[str]

    @classmethod
    def run(cls, *args: str, project_dir: str) -> "MeltanoCliInvocation":
        process = subprocess.Popen(
            [*args],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=project_dir,
        )
        return cls(process)


class MeltanoCliResource(dg.ConfigurableResource):
    """A resource that provides a Meltano CLI."""

    project_dir: str
    meltano_executable: str = "meltano"

    @public
    def cli(self, *args: str) -> MeltanoCliInvocation:
        return MeltanoCliInvocation.run(
            self.meltano_executable,
            *args,
            project_dir=self.project_dir,
        )
