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
    loader:
      name: target-jsonl
    description: "Extract data from CSV and load to JSONL"
    tags:
      environment: "dev"
```
