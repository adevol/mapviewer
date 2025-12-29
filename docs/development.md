# Development Guide

This project uses [invoke](https://www.pyinvoke.org/) for task automation and [pre-commit](https://pre-commit.com/) for code quality.

## Invoke Tasks

```bash
uv run invoke --list          # List all tasks
uv run invoke lint            # Run ruff linter with auto-fix
uv run invoke format          # Run ruff formatter
uv run invoke fix             # Run both lint + format
uv run invoke check           # Check without fixing
uv run invoke serve           # Start dev server
uv run invoke pipeline        # Run full data pipeline
uv run invoke pipeline --step etl  # Run specific step
```

## Pre-commit Hooks

Pre-commit is configured to run ruff on every commit.

### Setup

```bash
uv run pre-commit install
```

### Manual Run

```bash
uv run pre-commit run --all-files
```

## Code Style

This project uses [ruff](https://docs.astral.sh/ruff/) for linting and formatting. Configuration is in `pyproject.toml`.
