import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Callable

class Logger:
    """
    An instantiable logger that writes to timestamped files based on a fixed,
    aligned interval grid.

    The filename for any log message is always determined by the start time of the
    time interval in which the message is being written.
    """

    class _Logs:
        """
        Internal class to hold the state for a single named logger.
        It is responsible for creating and managing its own log file.
        """
        __slots__ = ('file', 'log_count')

        def __init__(self, directory: Path, interval_timestamp: str, name: str):
            """
            Initializes a log handle and creates the corresponding log file.
            The filename is a combination of the interval's start time and the logger name.
            """
            filename = f"{interval_timestamp}_{name}.log"
            file_path = directory / filename
            self.file = open(file_path, 'a', encoding='utf-8')
            self.log_count = 0

    def __init__(self,
                 log_dir: str,
                 interval_seconds: int = 24 * 60 * 60,
                 log_limit_per_file: int = 100_000,
                 max_log_msg_length: int = 10_000,
                 max_logger_name_length: int = 50):
        """
        Initializes a new, robust Logger instance.
        """
        if not isinstance(interval_seconds, int) or interval_seconds < 1:
            raise ValueError("`interval_seconds` must be an integer greater than or equal to 1.")

        self._directory = Path(log_dir)
        try:
            self._directory.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            raise ValueError(f"Invalid or unwritable log_dir: {log_dir}") from e

        # Configuration
        self._interval_ms: int = interval_seconds * 1_000
        self._log_limit_per_file = log_limit_per_file
        self._max_log_msg_length = max_log_msg_length
        self._max_logger_name_length = max_logger_name_length

        # State
        self._handles: Dict[str, Logger._Logs] = {}
        self._anchor_monotonic_ms: int = int(time.monotonic() * 1000)
        self._anchor_wall_ms: int = int(time.time() * 1000)

        # The monotonic start time of the interval for which files are currently open.
        self._active_interval_start_ms: int = 0
        self._interval_timestamp: str = ""

        # Set up the initial interval state immediately upon construction.
        # This ensures the first log file created has the correct timestamp.
        self._update_interval_if_needed(self._anchor_monotonic_ms)

    def _update_interval_if_needed(self, current_monotonic_ms: int):
        """
        Checks if the current time has crossed into a new interval. If so,
        it triggers a rotation by closing old files and calculating the new
        interval's timestamp.
        """
        time_since_anchor = current_monotonic_ms - self._anchor_monotonic_ms
        intervals_passed = time_since_anchor // self._interval_ms
        current_interval_start_ms = self._anchor_monotonic_ms + (intervals_passed * self._interval_ms)

        # If we are in a new interval, rotate.
        if current_interval_start_ms != self._active_interval_start_ms:
            self.close() # Close all old file handles.
            self._active_interval_start_ms = current_interval_start_ms

            # Translate the monotonic start of the interval to a wall clock time
            # for a human-readable filename.
            wall_monotonic_diff = self._anchor_wall_ms - self._anchor_monotonic_ms
            new_interval_start_wall_ms = current_interval_start_ms + wall_monotonic_diff
            
            interval_dt = datetime.fromtimestamp(new_interval_start_wall_ms / 1000)
            self._interval_timestamp = interval_dt.strftime('%Y-%m-%dT%H-%M-%S')

    def get_logger(self, name: str) -> Callable[[str, str, bool], None]:
        """Returns a logging function for a specific name."""
        if len(name) > self._max_logger_name_length:
            name = name[:self._max_logger_name_length]

        def log_writer(msg: str, level: str = "INFO", flush: bool = True):
            now_monotonic_ms = int(time.monotonic() * 1000)

            # STEP 1: Always check if the interval has changed and update if necessary.
            # This guarantees that `self._interval_timestamp` is correct for the
            # current moment in time before any file handles are accessed.
            self._update_interval_if_needed(now_monotonic_ms)

            # STEP 2: Get or create the file handle for this logger name.
            # Because of Step 1, if a new file is created here, it will use the
            # correct timestamp for the current interval.
            if name not in self._handles:
                self._handles[name] = self._Logs(
                    self._directory,
                    self._interval_timestamp,
                    name
                )
            
            handle = self._handles[name]

            # STEP 3: Write the log message.
            if handle.log_count >= self._log_limit_per_file:
                return
            
            if len(msg) > self._max_log_msg_length:
                msg = msg[:self._max_log_msg_length]

            now_wall_ms = int(time.time() * 1000)
            now_dt = datetime.fromtimestamp(now_wall_ms / 1000)
            now_str = now_dt.strftime('%Y-%m-%d %H:%M:%S') + f'.{now_dt.microsecond // 1000:03d}'
            log_line = f"[{now_str}] [{name.upper()}] [{level.upper()}] {msg}\n"
            
            handle.file.write(log_line)
            handle.log_count += 1
            
            if flush:
                handle.file.flush()

        return log_writer

    def close(self):
        """Closes all open log file handles, typically during rotation or shutdown."""
        for handle in self._handles.values():
            try:
                if not handle.file.closed:
                    handle.file.close()
            except Exception:
                pass
        self._handles.clear()