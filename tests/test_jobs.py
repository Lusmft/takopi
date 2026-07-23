from __future__ import annotations

import json
from pathlib import Path
import subprocess

import pytest

from takopi import jobs


def test_validate_job_id() -> None:
    assert jobs.validate_job_id("Deploy_V044") == "deploy_v044"
    with pytest.raises(jobs.JobError):
        jobs.validate_job_id("../escape")


def test_create_job_copies_script_and_spec(tmp_path: Path) -> None:
    script = tmp_path / "source.sh"
    script.write_text("#!/bin/bash\necho OK\n", encoding="utf-8")

    directory = jobs.create_job(
        job_id="deploy-v044",
        script_path=script,
        chat_id=123,
        timeout_s=60,
        title="deploy v0.4.4",
        root=tmp_path / "jobs",
    )

    assert (directory / "script.sh").read_text(encoding="utf-8") == (
        "#!/bin/bash\necho OK\n"
    )
    spec = json.loads((directory / "spec.json").read_text(encoding="utf-8"))
    assert spec["chat_id"] == 123
    assert spec["timeout_s"] == 60
    state = json.loads((directory / "state.json").read_text(encoding="utf-8"))
    assert state["status"] == "queued"


def test_launch_job_uses_transient_systemd_unit(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_run(command, **kwargs):
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, "", "")

    monkeypatch.setattr(jobs.subprocess, "run", fake_run)
    jobs.launch_job("deploy-v044", takopi_executable="/usr/local/bin/takopi")

    command = calls[0]
    assert command[:3] == [
        "systemd-run",
        "--quiet",
        "--collect",
    ]
    assert any(value.startswith("--unit=takopi-job-deploy-v044-") for value in command)
    assert command[-4:] == [
        "/usr/local/bin/takopi",
        "jobs",
        "worker",
        "deploy-v044",
    ]


def test_worker_records_success_and_notifies(monkeypatch, tmp_path: Path) -> None:
    script = tmp_path / "source.sh"
    script.write_text("echo deployed\n", encoding="utf-8")
    root = tmp_path / "jobs"
    jobs.create_job(
        job_id="deploy",
        script_path=script,
        chat_id=123,
        timeout_s=60,
        title="release",
        root=root,
    )
    notifications: list[tuple[int, str]] = []
    monkeypatch.setattr(
        jobs, "notify", lambda chat_id, text: notifications.append((chat_id, text))
    )

    assert jobs.run_worker("deploy", root=root) == 0

    result = jobs.read_json(root / "deploy" / "result.json")
    assert result is not None
    assert result["status"] == "succeeded"
    assert result["return_code"] == 0
    assert notifications[0][0] == 123
    assert "deployed" in notifications[0][1]


def test_worker_records_timeout(monkeypatch, tmp_path: Path) -> None:
    script = tmp_path / "source.sh"
    script.write_text("sleep 10\n", encoding="utf-8")
    root = tmp_path / "jobs"
    jobs.create_job(
        job_id="slow",
        script_path=script,
        chat_id=123,
        timeout_s=1,
        title=None,
        root=root,
    )
    monkeypatch.setattr(jobs, "notify", lambda *_args: None)

    assert jobs.run_worker("slow", root=root) == 124
    state = jobs.job_status("slow", root=root)
    assert state["status"] == "timed_out"


def test_duplicate_job_is_rejected(tmp_path: Path) -> None:
    script = tmp_path / "source.sh"
    script.write_text("true\n", encoding="utf-8")
    root = tmp_path / "jobs"
    jobs.create_job(
        job_id="same",
        script_path=script,
        chat_id=123,
        timeout_s=60,
        title=None,
        root=root,
    )
    with pytest.raises(jobs.JobError, match="already exists"):
        jobs.create_job(
            job_id="same",
            script_path=script,
            chat_id=123,
            timeout_s=60,
            title=None,
            root=root,
        )
