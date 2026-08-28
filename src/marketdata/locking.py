"""Shared process locking for data-directory mutation coordinators."""

from __future__ import annotations

import errno
import fcntl
import inspect
import json
import os
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import wraps
from pathlib import Path
from typing import ParamSpec, TypeVar

from marketdata.errors import DataDirectoryBusyError

LOCK_FILE_NAME = ".market-data.lock"
_MAX_OPERATION_LENGTH = 160
_MAX_HOLDER_BYTES = 1_024

P = ParamSpec("P")
R = TypeVar("R")


@dataclass
class _LocalLockState:
    mutex: threading.RLock
    depth: int = 0
    fd: int | None = None
    holder: dict[str, object] | None = None
    holder_guard: threading.Lock = field(default_factory=threading.Lock)


_registry_guard = threading.Lock()
_registry: dict[Path, _LocalLockState] = {}


def _reset_after_fork() -> None:
    """Drop inherited descriptors and thread ownership in a forked child."""
    global _registry_guard, _registry
    for state in _registry.values():
        if state.fd is not None:
            try:
                os.close(state.fd)
            except OSError:
                pass
    _registry_guard = threading.Lock()
    _registry = {}


os.register_at_fork(after_in_child=_reset_after_fork)


class DataDirectoryLock:
    """Exclusive, non-blocking advisory lock for one warehouse directory.

    The lock file is persistent so competing processes always contend on the
    same inode. Nested coordinators in one thread share the already-held OS
    lock; a competing thread or process fails immediately.
    """

    def __init__(self, data_dir: str | Path, *, operation: str):
        self.data_dir = Path(data_dir).resolve()
        self.lock_path = self.data_dir / LOCK_FILE_NAME
        self.operation = " ".join(operation.split())[:_MAX_OPERATION_LENGTH]
        self._state: _LocalLockState | None = None
        self._instance_depth = 0

    def __enter__(self) -> DataDirectoryLock:
        if not self.data_dir.is_dir():
            raise FileNotFoundError(
                f"data directory does not exist; initialize it first: {self.data_dir}"
            )
        with _registry_guard:
            state = _registry.setdefault(
                self.lock_path, _LocalLockState(threading.RLock())
            )
        if not state.mutex.acquire(blocking=False):
            with state.holder_guard:
                holder = state.holder
            raise DataDirectoryBusyError(self.lock_path, holder)
        if state.depth:
            state.depth += 1
            self._state = state
            self._instance_depth += 1
            return self

        try:
            fd = os.open(self.lock_path, os.O_RDWR | os.O_CREAT, 0o600)
        except OSError:
            state.mutex.release()
            raise
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            os.close(fd)
            state.mutex.release()
            if exc.errno not in {errno.EACCES, errno.EAGAIN}:
                raise
            raise DataDirectoryBusyError(
                self.lock_path, _read_holder(self.lock_path)
            ) from exc

        holder = {
            "pid": os.getpid(),
            "operation": self.operation,
            "acquired_at": datetime.now(UTC).isoformat(),
        }
        try:
            _write_holder(fd, holder)
        except Exception:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)
                state.mutex.release()
            raise
        state.fd = fd
        with state.holder_guard:
            state.holder = holder
        state.depth = 1
        self._state = state
        self._instance_depth = 1
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        state = self._state
        if state is None or self._instance_depth == 0:
            return
        state.depth -= 1
        self._instance_depth -= 1
        try:
            if state.depth == 0:
                assert state.fd is not None
                try:
                    _clear_holder(state.fd)
                finally:
                    try:
                        fcntl.flock(state.fd, fcntl.LOCK_UN)
                    finally:
                        os.close(state.fd)
                        state.fd = None
                        with state.holder_guard:
                            state.holder = None
        finally:
            if self._instance_depth == 0:
                self._state = None
            state.mutex.release()


def data_directory_locked(operation: str) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """Decorate a coordinator that receives a ``bars`` or ``meta`` store."""

    def decorate(function: Callable[P, R]) -> Callable[P, R]:
        signature = inspect.signature(function)

        @wraps(function)
        def locked(*args: P.args, **kwargs: P.kwargs) -> R:
            arguments = signature.bind(*args, **kwargs)
            data_dir = coordinated_data_directory(
                bars=arguments.arguments.get("bars"),
                meta=arguments.arguments.get("meta"),
            )
            with DataDirectoryLock(data_dir, operation=operation):
                return function(*args, **kwargs)

        return locked

    return decorate


def coordinated_data_directory(*, bars: object = None, meta: object = None) -> Path:
    """Resolve and validate the common data directory for coordinator stores."""
    bars_path = getattr(bars, "data_dir", None)
    meta_path = getattr(meta, "path", None)
    bars_dir = Path(bars_path).resolve() if bars_path is not None else None
    meta_dir = Path(meta_path).resolve().parent if meta_path is not None else None
    if bars_dir is not None and meta_dir is not None and bars_dir != meta_dir:
        raise ValueError(
            f"bar and metadata stores use different data directories: "
            f"{bars_dir}, {meta_dir}"
        )
    data_dir = bars_dir or meta_dir
    if data_dir is None:
        raise TypeError("locked coordinator requires a bars or meta store")
    return data_dir


def _read_holder(lock_path: Path) -> dict[str, object] | None:
    try:
        with lock_path.open("rb") as handle:
            raw = handle.read(_MAX_HOLDER_BYTES + 1)
    except OSError:
        return None
    if len(raw) > _MAX_HOLDER_BYTES:
        return None
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    holder: dict[str, object] = {}
    if isinstance(payload.get("pid"), int):
        holder["pid"] = payload["pid"]
    if isinstance(payload.get("operation"), str):
        holder["operation"] = payload["operation"][:_MAX_OPERATION_LENGTH]
    if isinstance(payload.get("acquired_at"), str):
        holder["acquired_at"] = payload["acquired_at"][:64]
    return holder or None


def _write_holder(fd: int, holder: dict[str, object]) -> None:
    payload = (json.dumps(holder, separators=(",", ":"), sort_keys=True) + "\n").encode(
        "utf-8"
    )
    if len(payload) > _MAX_HOLDER_BYTES:
        raise ValueError("lock-holder metadata exceeds its byte bound")
    os.lseek(fd, 0, os.SEEK_SET)
    os.ftruncate(fd, 0)
    remaining = memoryview(payload)
    while remaining:
        written = os.write(fd, remaining)
        if written == 0:
            raise OSError("could not write lock-holder metadata")
        remaining = remaining[written:]
    os.fsync(fd)


def _clear_holder(fd: int) -> None:
    os.lseek(fd, 0, os.SEEK_SET)
    os.ftruncate(fd, 0)
    os.fsync(fd)
