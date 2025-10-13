import os


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


def get_object_path(filepath: str, start_folder_name: str):
    """Construct object path without absolute path's pretext.

    Args:
        filepath: Absolute file path to upload.
        start_folder_name: Folder name to begin object path.

    Returns:
        str:
        Returns the object name.
    """
    # Split file_path into parts
    parts = filepath.split(os.sep)
    try:
        # Find index of the folder to start from
        start_index = parts.index(start_folder_name)
    except ValueError:
        # Folder not found in path, fallback to full path or raise error
        raise ValueError(f"Folder '{start_folder_name}' not found in path '{filepath}'")
    # Reconstruct path from start_folder_name onwards
    relative_parts = parts[start_index:]
    # Join with os.sep for system-appropriate separators
    return os.sep.join(relative_parts)
