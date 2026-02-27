# Justfile

build: update test clean

# Command to update all dependencies
update: update-github-actions update-deps update-pre-commit-hooks

# Command to update GitHub actions using pinact
update-github-actions:
    -pinact run --update

# Command to update pre-commit hooks
update-pre-commit-hooks:
    -uvx prek autoupdate

# Command to update dependencies
update-deps:
    uv lock --upgrade

test: pre-commit tox

# Run pre-commit checks
pre-commit:
    -uvx prek run --all-files

# Run all default nox sessions
tox:
    tox p

clean:
    rm -rf .tox/
    find . -type d -name '__pycache__' -exec rm -r {} +
