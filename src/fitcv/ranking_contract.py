"""@meta
name: ranking_contract
type: module
domain: runtime
ownership: feature
capabilities:
  - cv_system.stage-artifact-diagnostics
responsibility:
  - Shared ranking contracts for thresholds, fit-label mapping, and validation.
inputs:
  - Ranking config and score values
outputs:
  - Validated thresholds, labels, and invariant checks
lifecycle:
  - status: active
"""

from typing import Any

FIT_LABEL_STRONG = "strong"
FIT_LABEL_STRETCH = "stretch"
FIT_LABEL_SKIP = "skip"
VALID_FIT_LABELS = frozenset({FIT_LABEL_STRONG, FIT_LABEL_STRETCH, FIT_LABEL_SKIP})

DEFAULT_FIT_LABEL_STRONG_THRESHOLD = 0.70
DEFAULT_FIT_LABEL_STRETCH_THRESHOLD = 0.40


def _coerce_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def get_fit_label_thresholds(config: dict[str, Any] | None = None) -> tuple[float, float]:
    thresholds = dict((config or {}).get("fit_label_thresholds") or {})
    strong = _coerce_float(thresholds.get("strong"), DEFAULT_FIT_LABEL_STRONG_THRESHOLD)
    stretch = _coerce_float(thresholds.get("stretch"), DEFAULT_FIT_LABEL_STRETCH_THRESHOLD)
    if strong < stretch:
        raise ValueError(
            f"Invalid fit_label_thresholds: strong ({strong}) must be >= stretch ({stretch})."
        )
    return strong, stretch


def fit_label_from_score(score: float, config: dict[str, Any] | None = None) -> str:
    strong, stretch = get_fit_label_thresholds(config)
    if score >= strong:
        return FIT_LABEL_STRONG
    if score >= stretch:
        return FIT_LABEL_STRETCH
    return FIT_LABEL_SKIP


def validate_weight_contract(weights: dict[str, float], *, expected_sum: float = 1.0) -> None:
    total = 0.0
    for feature_name, value in weights.items():
        if value < 0.0 or value > 1.0:
            raise ValueError(
                f"Invalid ranking weight for '{feature_name}': {value}. Expected within [0.0, 1.0]."
            )
        total += value
    if abs(total - expected_sum) > 1e-6:
        raise ValueError(
            f"Invalid ranking weights sum: {total}. Expected {expected_sum}."
        )


def validate_missing_defaults_contract(
    defaults: dict[str, float],
    *,
    supported_features: tuple[str, ...],
) -> None:
    missing = [feature_name for feature_name in supported_features if feature_name not in defaults]
    if missing:
        raise ValueError(f"Missing defaults for ranking features: {missing}")
    for feature_name in supported_features:
        value = defaults[feature_name]
        if value < 0.0 or value > 1.0:
            raise ValueError(
                f"Invalid missing-value default for '{feature_name}': {value}. Expected within [0.0, 1.0]."
            )
