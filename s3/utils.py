import os


def getenv(*args, default: str = None) -> str:
    """Returns the key-ed environment variable or the default value."""
    for key in args:
        if value := os.environ.get(key.upper()) or os.environ.get(key.lower()):
            return value
    return default


def get_object_path(file_path, start_folder_name):
    # Split file_path into parts
    parts = file_path.split(os.sep)
    try:
        # Find index of the folder to start from
        start_index = parts.index(start_folder_name)
    except ValueError:
        # Folder not found in path, fallback to full path or raise error
        raise ValueError(f"Folder '{start_folder_name}' not found in path '{file_path}'")
    # Reconstruct path from start_folder_name onwards
    relative_parts = parts[start_index:]
    # Join with os.sep for system-appropriate separators
    return os.sep.join(relative_parts)
