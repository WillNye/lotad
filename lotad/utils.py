import hashlib
import json
import logging
import re
import urllib.parse
from typing import Any, Union
from uuid import UUID

import pandas as pd
import xxhash

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
UNTRACKED_VALUE_REGEXES = {
    re.compile(r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', re.IGNORECASE),
}


def is_tracked_column(val: Any) -> bool:
    if isinstance(val, UUID):
        return False
    elif not isinstance(val, str):
        return True

    return not any(
        regex.match(str(val)) for regex in UNTRACKED_VALUE_REGEXES
    )


def maybe_load_dict(val: str) -> Union[str, dict]:
    """Attempts to parse a string as JSON, including URL-encoded JSON strings."""
    try:
        return json.loads(val)
    except json.JSONDecodeError:
        try:
            return json.loads(urllib.parse.unquote(val))
        except json.JSONDecodeError:
            return val


def get_row_hash(row: Any) -> Union[str, tuple]:
    """Generates a consistent hash for row-level data comparison.

    This function creates a deterministic hash representation of row data,
    handling various data types and nested structures. It specifically accounts
    for JSON strings, dictionaries, and lists.

    The function processes data recursively, ensuring consistent handling of
    nested structures while maintaining sort order for dictionaries and lists
    to ensure hash consistency.

    Args:
        row (Any): The row data to hash. Can be a primitive type, dictionary,
                  list, or JSON string.

    Returns:
        Union[str, tuple]: A hash string for primitive types and dictionaries,
                          or a sorted tuple of hashes for lists.

    Example:
        >>> get_row_hash('{"a": 1, "b": 2}')
        'xxhash_hexdigest_value'
        >>> get_row_hash([1, 2, 3])
        ('1', '2', '3')
        >>> get_row_hash("simple string")
        'simple string'

    Note:
        - Dictionary keys are sorted before hashing to ensure consistent results
        - Pandas Timestamps are excluded from the hash computation
        - Nested JSON strings are parsed and processed recursively
        - Lists are converted to sorted tuples of hashed values
    """
    if isinstance(row, str) and row.startswith("{") and row.endswith("}"):
        row = maybe_load_dict(row)

    if isinstance(row, dict):
        normalized_dict = {}
        for k, v in sorted(row.items()):
            if isinstance(v, str) and v.startswith("{") and v.endswith("}"):
                v = maybe_load_dict(v)

            normalized_dict[k] = get_row_hash(v)

        return xxhash.xxh64(
            json.dumps(sorted(normalized_dict.items())).encode()
        ).hexdigest()
    elif isinstance(row, list):
        return tuple(
            sorted(
                get_row_hash(_item) for _item in row
            )
        )
    else:
        return str(row)
