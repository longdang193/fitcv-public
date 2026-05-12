"""
Filter Langfuse trace export files into an analysis-ready subset.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable

_RICH_STAGE_FAMILIES = {"normalize", "cv_analysis", "cv_generation"}


def _iter_rows(path: Path) -> Iterable[dict[str, Any]]:
    if path.suffix.lower() == ".jsonl":
        for line in path.read_text(encoding="utf-8").splitlines():
            line_clean = line.strip()
            if not line_clean:
                continue
            item = json.loads(line_clean)
            if isinstance(item, dict):
                yield item
        return
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        for item in payload:
            if isinstance(item, dict):
                yield item


def _decode_json_object(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        text = value.strip()
        if not text or text == "null":
            return None
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            return None
        if isinstance(parsed, dict):
            return parsed
    return None


def _is_analysis_ready(row: dict[str, Any]) -> bool:
    name = str(row.get("name") or "")
    if name.endswith(":rich_io"):
        rich_output = _decode_json_object(row.get("output")) or {}
        stage_family = str(rich_output.get("stage_family") or "").strip().lower()
        if stage_family and stage_family not in _RICH_STAGE_FAMILIES:
            return False
        return True
    input_block = _decode_json_object(row.get("input"))
    output_block = _decode_json_object(row.get("output"))
    return input_block is not None or output_block is not None


def main() -> int:
    parser = argparse.ArgumentParser(description="Filter Langfuse export to analysis-ready rows.")
    parser.add_argument("--input", required=True, help="Path to Langfuse export (.json or .jsonl)")
    parser.add_argument("--output", required=True, help="Output JSONL path")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    rows = list(_iter_rows(input_path))
    filtered = [row for row in rows if _is_analysis_ready(row)]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_lines = [json.dumps(row, ensure_ascii=False) for row in filtered]
    output_path.write_text("\n".join(output_lines) + ("\n" if output_lines else ""), encoding="utf-8")
    print(
        json.dumps(
            {
                "input_rows": len(rows),
                "filtered_rows": len(filtered),
                "output_path": str(output_path),
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
