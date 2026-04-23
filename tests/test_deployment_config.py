"""
@meta
type: test
scope: unit
domain: deployment
covers:
  - deployment configuration behavior
excludes:
  - live deployment provisioning
tags:
  - fast
  - ci-safe
"""

from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_docker_compose_mounts_runtime_config_files() -> None:
    compose = yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8"))
    services = compose["services"]

    for service_name in ("web", "worker"):
        volumes = services[service_name]["volumes"]
        assert any("/app/.env.yaml:ro" in volume for volume in volumes)
        assert any("/app/config/env.yaml:ro" in volume for volume in volumes)
        assert "fitcv_uploads:/app/data/uploads" in volumes

    assert compose["volumes"] == {"fitcv_uploads": None}


def test_dockerfile_copies_templates_directory() -> None:
    dockerfile = (REPO_ROOT / "Dockerfile").read_text(encoding="utf-8")
    assert "COPY templates/ ./templates/" in dockerfile
"""
@meta
type: test
scope: unit
domain: deployment
covers:
  - deployment configuration behavior
excludes:
  - live deployment provisioning
tags:
  - fast
  - ci-safe
"""
