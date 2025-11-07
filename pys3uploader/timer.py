import logging
import threading
from typing import Any, Callable, Dict, Tuple


class RepeatedTimer:
    """Instantiates RepeatedTimer object to kick off the threading.Timer object with custom intervals.

    >>> RepeatedTimer

    """

    def __init__(
        self,
        interval: int,
        function: Callable,
        args: Tuple = None,
        kwargs: Dict[str, Any] = None,
        logger: logging.Logger = None,
    ):
        """Repeats the ``Timer`` object from threading.

        Args:
            interval: Interval in seconds.
            function: Function to trigger with intervals.
            args: Arguments for the function.
            kwargs: Keyword arguments for the function.
            logger: Logger instance.
        """
        self.interval = interval
        self.function = function
        self.args = args or ()
        self.kwargs = kwargs or {}
        self.logger = logger or logging.getLogger(__name__)
        self.thread = None
        self._stop_event = threading.Event()

    def _run(self):
        """Triggers the target function."""
        while not self._stop_event.wait(self.interval):
            try:
                self.function(*self.args, **self.kwargs)
            except Exception as error:
                self.logger.error("Error in RepeatedTimer function [%s]: %s", self.function.__name__, error)

    def start(self):
        """Trigger target function if timer isn't running already."""
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def stop(self, timeout: int = 3):
        """Stop the timer and cancel all futures."""
        self._stop_event.set()
        if self.thread:
            self.thread.join(timeout=timeout)
