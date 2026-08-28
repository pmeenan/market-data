"""Shared data-directory process lock tests."""

import json
import threading

import pytest

from marketdata.locking import (
    LOCK_FILE_NAME,
    DataDirectoryBusyError,
    DataDirectoryLock,
    _read_holder,
)


def test_lock_is_reentrant_and_released_after_an_exception(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    lock = DataDirectoryLock(data_dir, operation="outer mutation")

    with pytest.raises(ValueError, match="simulated failure"):
        with lock:
            with lock:
                holder = json.loads((data_dir / LOCK_FILE_NAME).read_text())
                assert holder["operation"] == "outer mutation"
                assert isinstance(holder["pid"], int)
            raise ValueError("simulated failure")

    with DataDirectoryLock(data_dir, operation="retry"):
        holder = json.loads((data_dir / LOCK_FILE_NAME).read_text())
        assert holder["operation"] == "retry"
    acquired = []

    def acquire_from_other_thread() -> None:
        with DataDirectoryLock(data_dir, operation="thread retry"):
            acquired.append(True)

    thread = threading.Thread(target=acquire_from_other_thread)
    thread.start()
    thread.join(timeout=2)
    assert acquired == [True]
    assert (data_dir / LOCK_FILE_NAME).read_bytes() == b""


def test_competing_thread_fails_fast_with_bounded_holder_detail(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    errors = []

    def contend() -> None:
        try:
            with DataDirectoryLock(data_dir, operation="contender"):
                pass
        except DataDirectoryBusyError as exc:
            errors.append(exc)

    with DataDirectoryLock(data_dir, operation="thread owner"):
        thread = threading.Thread(target=contend)
        thread.start()
        thread.join(timeout=2)

    assert not thread.is_alive()
    assert len(errors) == 1
    assert "operation=thread owner" in str(errors[0])
    assert len(str(errors[0])) < 1_024


def test_lock_refuses_to_materialize_a_missing_data_directory(tmp_path):
    data_dir = tmp_path / "mistyped-data-dir"

    with pytest.raises(FileNotFoundError, match="initialize it first"):
        with DataDirectoryLock(data_dir, operation="reconcile"):
            pass

    assert not data_dir.exists()


def test_holder_metadata_limit_is_measured_in_bytes(tmp_path):
    lock_path = tmp_path / LOCK_FILE_NAME
    payload = json.dumps({"operation": "\N{ROCKET}" * 300}, ensure_ascii=False)
    lock_path.write_bytes(payload.encode("utf-8"))

    assert len(payload) < 1_024
    assert len(payload.encode("utf-8")) > 1_024
    assert _read_holder(lock_path) is None
