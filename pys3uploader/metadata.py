from dataclasses import dataclass
from typing import List


@dataclass
class Metadata(dict):
    """Dataclass for metadata information."""

    timestamp: str
    objects_uploaded: int
    objects_pending: int
    objects_failed: int
    size_uploaded: str
    size_pending: str
    size_failed: str
    success: List[str]
    failed: List[str]
