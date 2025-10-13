import os
from typing import Dict, Set


class UploadResults(dict):
    """Object to store results of S3 upload.

    >>> UploadResults

    """

    success: int = 0
    failed: int = 0


def getenv(*args, default: str = None) -> str:
    """Returns the key-ed environment variable or the default value."""
    for key in args:
        if value := os.environ.get(key.upper()) or os.environ.get(key.lower()):
            return value
    return default


def urljoin(*args) -> str:
    """Joins given arguments into a url. Trailing but not leading slashes are stripped for each argument.

    Returns:
        str:
        Joined url.
    """
    return "/".join(map(lambda x: str(x).rstrip("/").lstrip("/"), args))


def convert_to_folder_structure(sequence: Set[str]) -> str:
    """Convert objects in a s3 buckets into a folder like representation.

    Args:
        sequence: Takes either a mutable or immutable sequence as an argument.

    Returns:
        str:
        String representation of the architecture.
    """
    folder_structure = {}
    for item in sequence:
        parts = item.split("/")
        current_level = folder_structure
        for part in parts:
            current_level = current_level.setdefault(part, {})

    def generate_folder_structure(structure: Dict[str, dict], indent: str = "") -> str:
        """Generates the folder like structure.

        Args:
            structure: Structure of folder objects as key-value pairs.
            indent: Required indentation for the ASCII.
        """
        result = ""
        for i, (key, value) in enumerate(structure.items()):
            if i == len(structure) - 1:
                result += indent + "└── " + key + "\n"
                sub_indent = indent + "    "
            else:
                result += indent + "├── " + key + "\n"
                sub_indent = indent + "│   "
            if value:
                result += generate_folder_structure(value, sub_indent)
        return result

    return generate_folder_structure(folder_structure)
