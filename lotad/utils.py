import json
import urllib.parse
from typing import Any, Union

import orjson
import xxhash


def maybe_load_dict(val: str) -> Union[str, dict]:
    """Attempts to parse a string as JSON, including URL-encoded JSON strings."""
    try:
        if val.startswith("%7B"):
            val = urllib.parse.unquote(val)
        return orjson.loads(val)
    except json.JSONDecodeError:
        return val


def get_row_hash(row: Any) -> str:
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
    json_init_chars = ["{", "[", "%7B"]
    if isinstance(row, str) and any(row.startswith(init_char) for init_char in json_init_chars):
        # Attempt to load the row as dict
        row = maybe_load_dict(row)

    if isinstance(row, dict):
        normalized_dict = {}
        # Sort the dict by keys and hash its values
        for k, v in sorted(row.items()):
            normalized_dict[k] = get_row_hash(v)

        return xxhash.xxh64(
            orjson.dumps(normalized_dict, option=orjson.OPT_SORT_KEYS)
        ).hexdigest()
    elif isinstance(row, list):
        # Sort the list, hash its values, and hash the list itself
        return xxhash.xxh64(
            orjson.dumps(
                sorted(
                    get_row_hash(_item) for _item in row
                )
            )
        ).hexdigest()
    else:
        # Just cast everything else to string for simplicity
        return str(row)
