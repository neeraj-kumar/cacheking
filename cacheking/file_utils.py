from __future__ import annotations

import os
import tempfile

from pathlib import Path

def _write_atomic(path: Path, data: bytes) -> None:
    """Write data to a file atomically using a temporary file."""
    # create parent dirs if they don't exist
    path.parent.mkdir(parents=True, exist_ok=True)
    # Create temporary file in same directory to ensure atomic rename
    with tempfile.NamedTemporaryFile(
        mode='wb',
        dir=path.parent,
        prefix=path.name + '.',
        suffix='.tmp',
        delete=False
    ) as tf:
        tf.write(data)
        # Ensure all data is written to disk
        tf.flush()
        os.fsync(tf.fileno())

    # Atomic rename to final path
    os.replace(tf.name, path)

def _read_file(path: Path) -> bytes|None:
    """Read file contents, returning None if file doesn't exist or is invalid."""
    try:
        with open(path, 'rb') as f:
            return f.read()
    except (FileNotFoundError, IOError):
        return None
