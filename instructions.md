uvx create-dagster project dagster-pypistats

cd dagster-pypistats

source .venv/bin/activate

uv add --editable $HOME/Arch/dagster-meltano-pipelines/

meltano init meltano_project

$ dg list components --package dagster_meltano_pipelines
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Key                                                ┃ Summary                                         ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ dagster_meltano_pipelines.MeltanoPipelineComponent │ A component that represents a Meltano pipeline. │
└────────────────────────────────────────────────────┴─────────────────────────────────────────────────┘
