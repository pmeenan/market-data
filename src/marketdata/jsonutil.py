"""Canonical JSON encoding shared by durable hashes and catalog values."""

from __future__ import annotations

import json
import math
from collections.abc import Mapping
from typing import Any


def canonical_json(value: Any) -> str:
    """Encode a JSON value deterministically while preserving JSON types."""
    _validate_json_value(value)
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )


def _validate_json_value(value: Any) -> None:
    if value is None or isinstance(value, (str, bool, int)):
        return
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("canonical JSON numbers must be finite")
        return
    if isinstance(value, list):
        for item in value:
            _validate_json_value(item)
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            if not isinstance(key, str):
                raise TypeError("canonical JSON object keys must be strings")
            _validate_json_value(item)
        return
    raise TypeError(
        "canonical JSON values must be null, booleans, numbers, strings, "
        "lists, or string-keyed objects"
    )
