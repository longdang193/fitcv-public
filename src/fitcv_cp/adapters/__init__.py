"""@meta
name: __init__
type: module
domain: runtime
ownership: infrastructure
responsibility:
  - Module metadata placeholder for src.fitcv_cp.adapters.__init__.
inputs:
  - Internal runtime calls and module imports
outputs:
  - Module-level symbols and runtime behavior
lifecycle:
  - status: active
"""

from fitcv_cp.adapters.contracts import EmbeddingClient, LLMClient, RoutingSelection, resolve_part_routing

__all__ = [
    "LLMClient",
    "EmbeddingClient",
    "RoutingSelection",
    "resolve_part_routing",
]
