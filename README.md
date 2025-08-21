# dagster-meltano-pipelines

Dagster components for building reusable Meltano pipelines

## Installation

Add the package to your Dagster project using uv:

```bash
uv add dagster-meltano-pipelines
```

Or install from source:

```bash
uv add git+https://github.com/edgarrm358/dagster-meltano-pipelines.git
```

## Usage

### Scaffolding a Meltano Pipeline Component

You can scaffold a new Meltano pipeline component using the Dagster CLI:

#### With default project path

This creates a Meltano project in a `project/` directory relative to your component:

```bash
dg scaffold dagster_meltano_pipelines.MeltanoPipelineComponent my_meltano_pipelines
```

#### With custom project path

This creates a Meltano project in a custom directory at the root of your project:

```bash
dg scaffold dagster_meltano_pipelines.MeltanoPipelineComponent my_meltano_pipelines --project-path meltano
```

### Component Structure

The scaffolded component will create:

- A Dagster component definition file with your Meltano pipeline configuration
- A Meltano project directory with:
  - `meltano.yml` - Meltano project configuration
  - `logging.yaml` - Structured logging configuration
  - Default development, staging, and production environments

### Configuration

The component uses a YAML configuration file where you can define your Meltano pipelines:

```yaml
project: "{{ project_root }}/meltano"  # Path to your Meltano project
pipelines:
  my_pipeline:
    extractor:
      name: tap-csv
      git_ssh_private_key: "{{ env_var('EXTRACTOR_SSH_KEY') }}"  # SSH key for extractor Git authentication
    loader:
      name: target-jsonl
      git_ssh_private_key: "{{ env_var('LOADER_SSH_KEY') }}"  # SSH key for loader Git authentication
    description: "Extract data from CSV and load to JSONL"
    tags:
      environment: "dev"
```

#### Git SSH Authentication

For Git repositories that require SSH authentication, you can configure an SSH private key on individual plugins (extractors and loaders):

```yaml
pipelines:
  my_pipeline:
    extractor:
      name: tap-github
      git_ssh_private_key: "{{ env_var('GITHUB_SSH_KEY') }}"
    loader:
      name: target-postgres
      # No SSH key needed for this loader
```

**Note:** Pipeline-level `git_ssh_private_keys` configuration is deprecated. Configure `git_ssh_private_key` on individual extractor and loader plugins instead.
