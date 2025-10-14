import threading

from alive_progress.core.progress import __AliveBarHandle


class ProgressPercentage:
    """Tracks progress of a file upload to S3 and updates the alive_bar.

    >>> ProgressPercentage

    """

    def __init__(self, filename: str, size: int, bar: __AliveBarHandle):
        """Initializes the progress tracker.

        Args:
            filename: Name of the file being uploaded.
            size: Total size of the file in bytes.
            bar: alive_bar instance to update progress.
        """
        self._filename = filename
        self._size = size
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._bar = bar

    def __call__(self, bytes_amount: int) -> None:
        """Callback method to update progress.

        Args:
            bytes_amount: Number of bytes transferred in the last chunk.
        """
        with self._lock:
            self._seen_so_far += bytes_amount
            percent = (self._seen_so_far / self._size) * 100
            # Update alive_bar text with current file and % progress
            self._bar.text(f" || {self._filename} [{percent:.1f}%]")
