"""
@meta
type: test
scope: unit
domain: provider_routing
covers:
  - part-based provider/model routing resolution
excludes:
  - live provider network calls
tags:
  - fast
  - ci-safe
"""

import pytest

from fitcv.config import load_control_plane_config
from fitcv_cp.adapters.contracts import resolve_part_routing


def test_resolve_part_routing_reads_provider_and_model_from_control_plane() -> None:
    cfg = load_control_plane_config()

    selection = resolve_part_routing(cfg, "enrich_extraction")

    assert selection.provider == "openai_compatible"
    assert selection.model == "cx/gpt-5.2"


def test_resolve_part_routing_rejects_unknown_part() -> None:
    cfg = load_control_plane_config()

    with pytest.raises(ValueError, match="Unsupported model routing part"):
        resolve_part_routing(cfg, "unknown_part")


def test_resolve_part_routing_rejects_provider_missing_from_registry() -> None:
    cfg = {
        "providers": {"openai": {"base_url": "https://api.openai.com/v1"}},
        "model_routing": {
            "parts": {
                "enrich_extraction": {
                    "provider": "openai_compatible",
                    "model": "kimi-k2-instruct",
                }
            }
        },
    }

    with pytest.raises(ValueError, match="Unsupported provider"):
        resolve_part_routing(cfg, "enrich_extraction")
