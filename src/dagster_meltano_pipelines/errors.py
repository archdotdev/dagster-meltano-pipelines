from typing import List, Optional

import dagster as dg


class DagsterMeltanoError(dg.Failure):
    """The base exception of the ``dagster-meltano`` library."""


class DagsterMeltanoProjectNotFoundError(DagsterMeltanoError):
    """The project directory does not exist."""


class DagsterMeltanoPipelineError(DagsterMeltanoError):
    """Exception raised when a Meltano pipeline fails with error logs."""

    def __init__(
        self,
        message: str,
        exit_code: int,
        error_logs: Optional[List[str]] = None,
    ):
        self.exit_code = exit_code
        self.error_logs = error_logs or []

        enhanced_message = f"{message} (exit code: {exit_code})"
        if self.error_logs:
            enhanced_message += "\n\nMeltano error logs:\n" + "\n".join(self.error_logs[-5:])

        super().__init__(enhanced_message)
