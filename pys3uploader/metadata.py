from dataclasses import dataclass


@dataclass
class Metadata(dict):
    """Dataclass for metadata information."""

    timestamp: str
    objects_uploaded: int
    objects_pending: int
    size_uploaded: str
    size_pending: str
