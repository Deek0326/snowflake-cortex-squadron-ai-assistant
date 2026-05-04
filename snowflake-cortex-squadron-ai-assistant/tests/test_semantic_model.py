from __future__ import annotations

from pathlib import Path

import yaml


def test_semantic_model_has_verified_queries():
    path = Path("semantic_model/squadron_operations.semantic.yaml")
    semantic_model = yaml.safe_load(path.read_text())

    assert semantic_model["name"] == "SQUADRON_OPERATIONS"
    assert len(semantic_model["tables"]) >= 5
    assert len(semantic_model["verified_queries"]) >= 4

