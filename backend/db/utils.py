"""
Database utility functions.
"""

import hashlib


def generate_hash_key(*values) -> str:
    """
    Generate MD5 hash key for Data Vault hub/link.

    This creates deterministic hash keys from business keys.

    Args:
        *values: Values to hash (typically business key components)

    Returns:
        32-character MD5 hash (lowercase hex)

    Example:
        >>> generate_hash_key("Sugarloaf", "ME")
        'a1b2c3d4e5f6...'
    """
    # Combine values with pipe delimiter
    combined = "|".join(str(v) for v in values)

    # Generate MD5 hash
    return hashlib.md5(combined.encode()).hexdigest()
