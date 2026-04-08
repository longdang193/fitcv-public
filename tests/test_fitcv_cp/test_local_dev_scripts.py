from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


def test_windows_local_dev_scripts_exist_and_use_simple_worker() -> None:
    expected_scripts = [
        "start_web.ps1",
        "start_worker.ps1",
        "stop_fitcv.ps1",
    ]

    for script_name in expected_scripts:
        script_path = REPO_ROOT / script_name
        assert script_path.exists(), f"Missing helper script: {script_name}"

    worker_script = (REPO_ROOT / "start_worker.ps1").read_text(encoding="utf-8")
    web_script = (REPO_ROOT / "start_web.ps1").read_text(encoding="utf-8")

    assert ".venv\\Scripts\\python.exe" in web_script
    assert "job-project-worker-1" in web_script
    assert "SimpleWorker" in worker_script
    assert "fitcv_cp.queue" in worker_script
    assert ".venv\\Scripts\\python.exe" in worker_script
    assert "job-project-worker-1" in worker_script
