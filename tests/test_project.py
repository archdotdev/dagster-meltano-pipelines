from pathlib import Path

import pytest
import yaml

from dagster_meltano_pipelines.project import MeltanoProject


def _write_meltano_yml(path: Path, content: dict) -> None:
    with open(path, "w") as f:
        yaml.dump(content, f)


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    _write_meltano_yml(
        tmp_path / "meltano.yml",
        {
            "plugins": {
                "extractors": [
                    {"name": "tap-base", "namespace": "tap_base"},
                ],
            },
        },
    )
    return tmp_path


def test_include_paths_literal(project_dir: Path) -> None:
    include_file = project_dir / "include.yml"
    _write_meltano_yml(
        include_file,
        {
            "plugins": {
                "extractors": [
                    {"name": "tap-included", "namespace": "tap_included"},
                ],
            },
        },
    )
    _write_meltano_yml(
        project_dir / "meltano.yml",
        {
            "include_paths": ["include.yml"],
            "plugins": {
                "extractors": [
                    {"name": "tap-base", "namespace": "tap_base"},
                ],
            },
        },
    )

    project = MeltanoProject(project_dir)
    assert ("extractors", "tap-base") in project.plugins
    assert ("extractors", "tap-included") in project.plugins


def test_include_paths_glob(project_dir: Path) -> None:
    includes_dir = project_dir / "includes"
    includes_dir.mkdir()

    for i in range(3):
        _write_meltano_yml(
            includes_dir / f"part{i}.yml",
            {
                "plugins": {
                    "extractors": [
                        {"name": f"tap-part{i}", "namespace": f"tap_part{i}"},
                    ],
                },
            },
        )

    _write_meltano_yml(
        project_dir / "meltano.yml",
        {
            "include_paths": ["includes/*.yml"],
            "plugins": {
                "extractors": [
                    {"name": "tap-base", "namespace": "tap_base"},
                ],
            },
        },
    )

    project = MeltanoProject(project_dir)
    assert ("extractors", "tap-base") in project.plugins
    for i in range(3):
        assert ("extractors", f"tap-part{i}") in project.plugins


def test_include_paths_glob_no_match(project_dir: Path) -> None:
    _write_meltano_yml(
        project_dir / "meltano.yml",
        {
            "include_paths": ["nonexistent/*.yml"],
            "plugins": {
                "extractors": [
                    {"name": "tap-base", "namespace": "tap_base"},
                ],
            },
        },
    )

    project = MeltanoProject(project_dir)
    assert ("extractors", "tap-base") in project.plugins
    assert len(project.plugins) == 1
