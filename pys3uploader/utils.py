import math
import os
from typing import Dict, List, Set

from botocore.config import Config

RETRY_CONFIG: Config = Config(
    retries={
        "max_attempts": 10,
        "mode": "adaptive",  # Adaptive retry mode with jitter
        "total_max_attempts": 20,  # Max retries across all requests
    },
    # Adding custom timeouts here:
    connect_timeout=5,  # 5 seconds for establishing a connection
    read_timeout=30,  # 30 seconds to wait for a response from the server
)


class UploadResults(dict):
    """Object to store results of S3 upload.

    >>> UploadResults

    """

    success: List[str] = []
    failed: List[str] = []
    skipped: List[str] = []


def getenv(*args, default: str = None) -> str:
    """Returns the key-ed environment variable or the default value.

    Args:
        args: Environment variable keys to search for.
        default: Default value to return if no environment variable is found.

    Returns:
        str:
        Environment variable value or the default value.
    """
    for key in args:
        if value := os.environ.get(key.upper()) or os.environ.get(key.lower()):
            return value
    return default


def urljoin(*args) -> str:
    """Joins given arguments into a url. Trailing but not leading slashes are stripped for each argument.

    Args:
        args: Parts of the url to join.

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

        Returns:
            str:
            String representation of the folder structure.
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


def convert_seconds(seconds: int | float, n_elem: int = 2) -> str:
    """Calculate years, months, days, hours, minutes, seconds, and milliseconds from given input.

    Args:
        seconds: Number of seconds to convert (supports float values).
        n_elem: Number of elements required from the converted list.

    Returns:
        str:
        Returns a humanized string notion of the number of seconds.
    """
    if not seconds:
        return "0s"
    elif seconds < 1:
        return f"{seconds * 1000:.0f}ms"

    seconds_in_year = 365 * 24 * 3600
    seconds_in_month = 30 * 24 * 3600

    years = seconds // seconds_in_year
    seconds %= seconds_in_year

    months = seconds // seconds_in_month
    seconds %= seconds_in_month

    days = seconds // (24 * 3600)
    seconds %= 24 * 3600

    hours = seconds // 3600
    seconds %= 3600

    minutes = seconds // 60
    seconds %= 60

    milliseconds = round((seconds % 1) * 1000)
    seconds = int(seconds)  # Convert remaining seconds to int for display

    time_parts = []

    if years > 0:
        time_parts.append(f"{int(years)} year{'s' if years > 1 else ''}")
    if months > 0:
        time_parts.append(f"{int(months)} month{'s' if months > 1 else ''}")
    if days > 0:
        time_parts.append(f"{int(days)} day{'s' if days > 1 else ''}")
    if hours > 0:
        time_parts.append(f"{int(hours)} hour{'s' if hours > 1 else ''}")
    if minutes > 0:
        time_parts.append(f"{int(minutes)} minute{'s' if minutes > 1 else ''}")
    if seconds > 0 or milliseconds > 0:
        if seconds > 0 and milliseconds > 0:
            time_parts.append(f"{seconds + milliseconds / 1000:.1f}s")
        elif seconds > 0:
            time_parts.append(f"{seconds}s")
        else:
            time_parts.append(f"{milliseconds}ms")

    if len(time_parts) == 1:
        return time_parts[0]

    list_ = time_parts[:n_elem]
    return ", and ".join([", ".join(list_[:-1]), list_[-1]] if len(list_) > 2 else list_)


def format_nos(input_: float) -> int | float:
    """Removes ``.0`` float values.

    Args:
        input_: Strings or integers with ``.0`` at the end.

    Returns:
        int | float:
        Int if found, else returns the received float value.
    """
    return int(input_) if isinstance(input_, float) and input_.is_integer() else input_


def size_converter(byte_size: int | float) -> str:
    """Gets the current memory consumed and converts it to human friendly format.

    Args:
        byte_size: Receives byte size as argument.

    Returns:
        str:
        Converted understandable size.
    """
    if not byte_size:
        return "0 B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    index = int(math.floor(math.log(byte_size, 1024)))
    return f"{format_nos(round(byte_size / pow(1024, index), 2))} {size_name[index]}"
