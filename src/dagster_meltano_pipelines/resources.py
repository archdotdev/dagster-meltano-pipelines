import abc
import collections.abc
import json
from typing import Any, Dict, List, Literal, Optional, Sequence

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
    use_cached_catalog: Optional[bool] = Field(
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
    git_ssh_private_key: Optional[str] = Field(
        default=None,
        description="SSH private key content for Git authentication",
    )

    def as_env(self) -> Dict[str, str]:
        """Convert the plugin configuration to a dictionary of environment variables."""
        env: Dict[str, str] = {}
        if not self.config:
            return env

        prefix = self.name.upper().replace("-", "_")

        for key, value in self.config.model_dump().items():
            suffix = key.upper()

            # Retrieve the value from the environment variable
            if isinstance(value, dg.EnvVar):
                value = value.get_value()

            # Convert sequence values to JSON strings after resolving environment variables
            elif isinstance(value, (list, tuple)):
                value = json.dumps(_resolve_sequence(value))

            # Convert object values to JSON strings
            elif isinstance(value, collections.abc.Mapping):
                value = json.dumps(_resolve_mapping(value))

            if value is not None:
                env[f"{prefix}_{suffix}"] = str(value)

        return env


class Extractor(MeltanoPlugin):
    """Extractor."""

    config: Optional[ExtractorConfig] = Field(description="The Meltano extractor configuration")


class Loader(MeltanoPlugin):
    """Loader."""


def _dict_to_env(value: collections.abc.Mapping[str, Any], *, prefix: Optional[str] = None) -> Dict[str, str]:
    env: Dict[str, str] = {}
    for k, v in value.items():
        key = f"{prefix}_{k.upper()}" if prefix else k.upper()
        if isinstance(v, dg.EnvVar):
            v = v.get_value()
        elif isinstance(v, (list, tuple)):
            v = json.dumps(_resolve_sequence(v))
        elif isinstance(v, collections.abc.Mapping):
            env |= _dict_to_env(v, prefix=key)
            continue

        if v is not None:
            env[key] = str(v)

    return env


def _resolve_mapping(value: collections.abc.Mapping[str, Any]) -> dict[str, Any]:
    """Resolve a mapping to a dictionary of environment variables."""
    result: dict[str, Any] = {}
    for k, v in value.items():
        if isinstance(v, dg.EnvVar):
            result[k] = v.get_value()
        elif isinstance(v, (list, tuple)):
            result[k] = _resolve_sequence(v)
        elif isinstance(v, collections.abc.Mapping):
            result[k] = _resolve_mapping(v)
        else:
            result[k] = v
    return result


def _resolve_sequence(value: Sequence[Any]) -> list[Any]:
    """Resolve a sequence to a list."""
    result: list[Any] = []
    for item in value:
        if isinstance(item, dg.EnvVar):
            result.append(item.get_value())
        elif isinstance(item, (list, tuple)):
            result.append(_resolve_sequence(item))
        elif isinstance(item, collections.abc.Mapping):
            result.append(_resolve_mapping(item))
        else:
            result.append(item)

    return result


class AsEnv(dg.PermissiveConfig):
    """Mixin for converting the configuration to a dictionary of environment variables."""

    @property
    @abc.abstractmethod
    def env_prefix(self) -> str:
        """The environment variable prefix."""
        ...

    def as_env(self) -> Dict[str, str]:
        """Convert the configuration to a dictionary of environment variables."""
        return _dict_to_env(self.model_dump(), prefix=self.env_prefix)


class StateBackendConfig(AsEnv):
    """State backend."""

    uri: str = Field(description="State backend URI")

    @property
    def env_prefix(self) -> str:
        """The environment variable prefix."""
        return "STATE_BACKEND"


class VenvConfig(AsEnv):
    """Venv backend."""

    backend: Literal["virtualenv", "uv"] = Field(description="Virtual Environment backend")

    @property
    def env_prefix(self) -> str:
        """The environment variable prefix."""
        return "VENV"


class CLIConfig(AsEnv):
    """CLI configuration."""

    log_level: Optional[Literal["debug", "info", "warning", "error", "critical"]] = Field(description="Log level")
    log_format: Optional[Literal["json", "text"]] = Field(description="Log format")

    @property
    def env_prefix(self) -> str:
        """The environment variable prefix."""
        return "CLI"


class ELTConfig(AsEnv):
    """ELT configuration."""

    buffer_size: Optional[int] = Field(description="Buffer size")

    @property
    def env_prefix(self) -> str:
        """The environment variable prefix."""
        return "ELT"


class MeltanoConfig(AsEnv):
    """Plugin configuration."""

    state_backend: Optional[StateBackendConfig] = Field(default=None, description="State backend")
    venv: Optional[VenvConfig] = Field(default=None, description="Virtual Environment configuration")
    cli: Optional[CLIConfig] = Field(default=None, description="CLI configuration")
    elt: Optional[ELTConfig] = Field(default=None, description="ELT configuration")

    @property
    def env_prefix(self) -> str:
        """The environment variable prefix."""
        return "MELTANO"

    def as_env(self) -> Dict[str, str]:
        """Convert the plugin configuration to a dictionary of environment variables."""
        env: Dict[str, str] = {}
        if self.state_backend:
            for key, value in self.state_backend.as_env().items():
                env[f"{self.env_prefix}_{key}"] = value

        if self.venv:
            for key, value in self.venv.as_env().items():
                env[f"{self.env_prefix}_{key}"] = value

        if self.cli:
            for key, value in self.cli.as_env().items():
                env[f"{self.env_prefix}_{key}"] = value

        if self.elt:
            for key, value in self.elt.as_env().items():
                env[f"{self.env_prefix}_{key}"] = value

        for key, value in self.model_dump(exclude={"state_backend", "venv", "cli", "elt"}).items():
            suffix = key.upper()
            if isinstance(value, dg.EnvVar):
                value = value.get_value()  # type: ignore[assignment]

            elif isinstance(value, (collections.abc.Mapping, list, tuple)):
                value = json.dumps(value)

            if value is not None:
                env[f"{self.env_prefix}_{suffix}"] = str(value)

        return env
