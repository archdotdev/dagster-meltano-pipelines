import dagster as dg


class DagsterMeltanoError(dg.Failure):
    """The base exception of the ``dagster-meltano`` library."""


class DagsterMeltanoProjectNotFoundError(DagsterMeltanoError):
    """The project directory does not exist."""
