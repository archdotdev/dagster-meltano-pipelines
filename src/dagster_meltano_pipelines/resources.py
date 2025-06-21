from typing import Any, Dict, List, Optional

import dagster as dg
from pydantic import Field


class MeltanoPluginConfig(dg.PermissiveConfig):
    """Plugin configuration."""

    def model_dump(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        kwargs.setdefault("by_alias", True)
        return super().model_dump(*args, **kwargs)


class ExtractorConfig(MeltanoPluginConfig):
    """Extractor configuration."""

    # https://github.com/meltano/meltano/blob/0dad7d51a36862e3df7a9f5cf19425540577e5c9/src/meltano/core/plugin/singer/tap.py#L172
    catalog: Optional[str] = Field(
        description="Stream catalog file",
        alias="_catalog",
    )
    state: Optional[str] = Field(
        description="Stream state file",
        alias="_state",
    )
    load_schema: Optional[str] = Field(
        description="Load data to this schema in the target database",
        alias="_load_schema",
    )
    select: Optional[List[str]] = Field(
        description="Stream selection",
        alias="_select",
    )
    metadata: Optional[Dict[str, Any]] = Field(
        description="Stream metadata",
        alias="_metadata",
    )
    tap_schema: Optional[Dict[str, Any]] = Field(
        description="Stream schema",
        alias="_schema",
    )
    select_filter: Optional[List[str]] = Field(
        description="Stream selection filter",
        alias="_select_filter",
    )
    use_cached_catalog: bool = Field(
        default=True,
        description="Use cached catalog",
        alias="_use_cached_catalog",
    )


class LoaderConfig(MeltanoPluginConfig):
    """Loader configuration."""

    # https://github.com/meltano/meltano/blob/0dad7d51a36862e3df7a9f5cf19425540577e5c9/src/meltano/core/plugin/singer/target.py#L105
    dialect: Optional[str] = Field(
        description="Target database dialect",
        alias="_dialect",
    )


class MeltanoPlugin(dg.ConfigurableResource["MeltanoPlugin"]):
    """Base class for Meltano plugins."""

    name: str = Field(description="The Meltano plugin name")
    config: Optional[MeltanoPluginConfig] = Field(description="The Meltano plugin configuration")


class Extractor(MeltanoPlugin):
    """Extractor."""

    config: Optional[ExtractorConfig] = Field(description="The Meltano extractor configuration")


class Loader(MeltanoPlugin):
    """Loader."""
