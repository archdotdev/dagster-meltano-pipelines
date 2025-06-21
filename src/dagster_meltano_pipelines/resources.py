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

    select: Optional[List[str]] = Field(description="Stream selection", alias="_select")
    metadata: Optional[Dict[str, Any]] = Field(
        description="Stream metadata",
        alias="_metadata",
    )


class MeltanoPlugin(dg.ConfigurableResource["MeltanoPlugin"]):
    """Base class for Meltano plugins."""

    name: str = Field(description="The Meltano plugin name")
    config: Optional[MeltanoPluginConfig] = Field(
        description="The Meltano plugin configuration",
    )


class Extractor(MeltanoPlugin):
    """Extractor."""

    config: Optional[ExtractorConfig] = Field(
        description="The Meltano extractor configuration",
    )


class Loader(MeltanoPlugin):
    """Loader."""
