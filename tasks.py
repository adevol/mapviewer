"""Invoke tasks for MapViewer development."""

from invoke import task


@task
def lint(c):
    """Run ruff linter with auto-fix."""
    c.run("uv run ruff check --fix .")


@task
def format(c):
    """Run ruff formatter."""
    c.run("uv run ruff format .")


@task
def check(c):
    """Run linting and formatting checks (no fixes)."""
    c.run("uv run ruff check .")
    c.run("uv run ruff format --check .")


@task(pre=[lint, format])
def fix(c):
    """Run all auto-fixes (lint + format)."""
    pass


@task
def serve(c):
    """Start the development server."""
    c.run("uv run uvicorn src.backend.main:app --reload")


@task
def pipeline(c, step=None):
    """Run the data pipeline.

    Args:
        step: Optional step to run (etl, precompute, split).
    """
    cmd = "uv run python -m src.data.pipeline"
    if step:
        cmd += f" --step {step}"
    c.run(cmd)
