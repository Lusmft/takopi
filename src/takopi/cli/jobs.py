from __future__ import annotations

# ruff: noqa: B008

import json
from pathlib import Path

import typer

from ..jobs import (
    JobError,
    cancel_job,
    job_status,
    list_jobs,
    run_worker,
    start_job,
)


jobs_app = typer.Typer(help="Manage durable jobs that survive agent run completion.")


def _fail(exc: JobError) -> None:
    typer.echo(f"error: {exc}", err=True)
    raise typer.Exit(code=1) from exc


@jobs_app.command("start")
def jobs_start(
    job_id: str = typer.Argument(..., help="Unique job identifier."),
    script: Path = typer.Option(
        ..., "--script", exists=True, dir_okay=False, readable=True
    ),
    chat_id: int = typer.Option(..., "--chat-id"),
    timeout_s: int = typer.Option(3600, "--timeout", min=1),
    title: str | None = typer.Option(None, "--title"),
) -> None:
    """Copy a script into Takopi state and run it in a transient systemd unit."""
    try:
        directory = start_job(
            job_id=job_id,
            script_path=script,
            chat_id=chat_id,
            timeout_s=timeout_s,
            title=title,
        )
    except JobError as exc:
        _fail(exc)
    typer.echo(f"started {job_id} ({directory})")


@jobs_app.command("status")
def jobs_status(job_id: str) -> None:
    """Show the latest durable job state."""
    try:
        typer.echo(json.dumps(job_status(job_id), indent=2, sort_keys=True))
    except JobError as exc:
        _fail(exc)


@jobs_app.command("list")
def jobs_list() -> None:
    """List durable jobs, newest first."""
    typer.echo(json.dumps(list_jobs(), indent=2, sort_keys=True))


@jobs_app.command("cancel")
def jobs_cancel(job_id: str) -> None:
    """Stop a running durable job."""
    try:
        cancel_job(job_id)
    except (JobError, OSError) as exc:
        _fail(JobError(str(exc)))
    typer.echo(f"canceled {job_id}")


@jobs_app.command("worker", hidden=True)
def jobs_worker(job_id: str) -> None:
    """Execute a durable job inside its transient systemd unit."""
    try:
        return_code = run_worker(job_id)
    except JobError as exc:
        _fail(exc)
    if return_code:
        raise typer.Exit(code=return_code)
