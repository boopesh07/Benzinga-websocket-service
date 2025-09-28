import logging
import os
import socket
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)


class FileWindowedWriter:
    def __init__(self, *, base_dir: str, window_minutes: int = 30, max_object_bytes: int = 512_000_000, use_marker: bool = True) -> None:
        self.base_dir = base_dir
        self.window_minutes = window_minutes
        self.max_object_bytes = max_object_bytes
        self.use_marker = use_marker
        self.hostname = socket.gethostname()

        os.makedirs(self.base_dir, exist_ok=True)

        self._current_window_start = None
        self._current_window_end = None
        self._seq_counter = 0
        self._file_path = None
        self._file = None
        self._bytes_written = 0

    def _floor_window(self, when: datetime) -> datetime:
        minute = (when.minute // self.window_minutes) * self.window_minutes
        return when.replace(minute=minute, second=0, microsecond=0, tzinfo=timezone.utc)

    def _window_bounds(self, when: datetime) -> tuple[datetime, datetime]:
        start = self._floor_window(when)
        end = start + timedelta(minutes=self.window_minutes) - timedelta(seconds=1)
        return start, end

    def _prefix_for_window(self, start: datetime, end: datetime) -> str:
        date_prefix = start.strftime("ingest_dt=%Y/%m/%d/hour=%H")
        part_prefix = f"part={start.strftime('%Y%m%d_%H%M')}-{end.strftime('%Y%m%d_%H%M')}"
        return os.path.join(self.base_dir, date_prefix, part_prefix)

    def _object_name(self) -> str:
        self._seq_counter += 1
        return f"{self.hostname}-seq={self._seq_counter:06d}.ndjson"

    def _start_new_window(self, when: datetime) -> None:
        start, end = self._window_bounds(when)
        prefix = self._prefix_for_window(start, end)
        os.makedirs(prefix, exist_ok=True)
        file_name = self._object_name()
        self._file_path = os.path.join(prefix, file_name)
        self._current_window_start = start
        self._current_window_end = end
        logger.debug("file-window-start path=%s", self._file_path)
        if self.use_marker:
            try:
                open(os.path.join(prefix, "uploading.marker"), "wb").close()
            except Exception:
                logger.exception("file-marker-put-failed prefix=%s", prefix)
        self._file = open(self._file_path, "ab")
        self._bytes_written = 0

    def _should_rotate(self, when: datetime) -> bool:
        if self._current_window_end is None:
            return True
        if when > self._current_window_end:
            return True
        if self._bytes_written >= self.max_object_bytes:
            return True
        return False

    def write_line(self, line: str) -> None:
        now = datetime.now(timezone.utc)
        if self._should_rotate(now):
            self._finalize_current()
            self._start_new_window(now)
        data = line.encode("utf-8")
        self._file.write(data)
        self._file.flush()
        self._bytes_written += len(data)

    def _finalize_current(self) -> None:
        if self._file is None:
            return
        try:
            try:
                self._file.flush()
                self._file.close()
            except Exception:
                logger.exception("file-close-failed path=%s", self._file_path)
        finally:
            if self.use_marker and self._current_window_start and self._current_window_end:
                prefix = self._prefix_for_window(self._current_window_start, self._current_window_end)
                marker = os.path.join(prefix, "uploading.marker")
                try:
                    if os.path.exists(marker):
                        os.remove(marker)
                except Exception:
                    logger.exception("file-marker-delete-failed marker=%s", marker)
            self._file = None
            self._file_path = None
            self._bytes_written = 0

    def close(self) -> None:
        self._finalize_current()
