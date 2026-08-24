# ruff: noqa: CPY001, S101
"""Tests for incremental and open GRN receipt streams."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Protocol, cast

if TYPE_CHECKING:
    from collections.abc import Iterator
    from pathlib import Path

from tap_easyecom.client import EasyEcomStream
from tap_easyecom.streams import OpenReceiptsStream, ReceiptsStream
from tap_easyecom.tap import TapEasyEcom


class MonkeyPatch(Protocol):
    """Minimal monkeypatch protocol used by these tests."""

    def setattr(self, target: object, name: str, value: object) -> None:
        """Replace an attribute for the duration of a test."""
        ...


def make_tap(state: dict[str, Any] | None = None) -> SimpleNamespace:
    """Build the minimal tap surface required by Singer SDK streams."""
    return SimpleNamespace(
        name="tap-easyecom",
        logger=logging.getLogger("tap-easyecom-tests"),
        config={},
        state=state or {},
        open_grn_ids_cache=set(),
    )


def test_receipts_cache_only_unfinished_grns() -> None:
    """The incremental stream should cache only unfinished GRNs."""
    tap = make_tap()
    stream = ReceiptsStream(cast("Any", tap))

    stream.post_process(
        {"grn_id": 101, "grn_status_id": 2, "grn_status": "In Progress"}
    )
    stream.post_process(
        {"grn_id": 102, "grn_status_id": 5, "grn_status": "Completed"}
    )
    stream.post_process(
        {"grn_id": 103, "grn_status_id": 5, "grn_status": "In Progress"}
    )

    assert tap.open_grn_ids_cache == {"101", "103"}


def test_receipts_replays_last_hotglue_day_after_a_gap() -> None:
    """A stale HotGlue timestamp should replay its full UTC day."""
    now = datetime.now(timezone.utc)
    last_modified = now - timedelta(days=1)
    tap = make_tap(
        {
            "hg_last_modified": last_modified.isoformat(),
            "bookmarks": {
                "receipts": {
                    "replication_key": "grn_created_at",
                    "replication_key_value": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "starting_replication_value": now.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                }
            },
        }
    )

    params = ReceiptsStream(cast("Any", tap)).get_url_params(None, None)

    assert params["created_after"] == last_modified.strftime("%Y-%m-%d 00:00:00")


def test_receipts_keeps_bookmark_when_hotglue_ran_today() -> None:
    """A same-day HotGlue timestamp should not change the receipt cursor."""
    now = datetime.now(timezone.utc)
    bookmark = now.replace(hour=13, minute=53, second=17, microsecond=0)
    tap = make_tap(
        {
            "hg_last_modified": now.isoformat(),
            "bookmarks": {
                "receipts": {
                    "replication_key": "grn_created_at",
                    "replication_key_value": bookmark.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                    "starting_replication_value": bookmark.strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                }
            },
        }
    )

    params = ReceiptsStream(cast("Any", tap)).get_url_params(None, None)

    assert params["created_after"] == bookmark.strftime("%Y-%m-%d %H:%M:%S")


def test_open_receipts_uses_grn_ids_without_created_after() -> None:
    """The replay stream should query GRN IDs without a date bookmark."""
    stream = OpenReceiptsStream(cast("Any", make_tap()))

    params = stream.get_url_params({"grn_ids": [101, 102]}, None)

    assert params == {"grn_ids": "101,102"}
    assert "created_after" not in params


def test_open_receipts_persists_only_unfinished_and_missing_ids(
    monkeypatch: MonkeyPatch,
) -> None:
    """Completed IDs should leave state while open and missing IDs remain."""
    tap = make_tap(
        {
            "bookmarks": {
                "open_receipts": {"grn_ids": [101, 102, 104]},
            }
        }
    )
    tap.open_grn_ids_cache = {"103"}
    stream = OpenReceiptsStream(cast("Any", tap))
    calls = []
    responses = {
        "101": {"grn_id": 101, "grn_status_id": 5, "grn_status": "Completed"},
        "102": {"grn_id": 102, "grn_status_id": 2, "grn_status": "In Progress"},
        "103": {"grn_id": 103, "grn_status_id": 5, "grn_status": "Completed"},
        # 104 is intentionally absent and must remain in state for retry.
    }

    def fake_get_records(
        _stream: EasyEcomStream, context: dict[str, list[str]]
    ) -> Iterator[dict[str, Any]]:
        calls.append(context["grn_ids"])
        for grn_id in context["grn_ids"]:
            record = responses.get(str(grn_id))
            if record:
                yield record

    monkeypatch.setattr(EasyEcomStream, "get_records", fake_get_records)

    records = list(stream.get_records(None))

    assert {record["grn_id"] for record in records} == {101, 102, 103}
    assert calls == [["101", "102", "103", "104"]]
    assert stream.stream_state["grn_ids"] == ["102", "104"]
    assert tap.open_grn_ids_cache == set()


def test_open_receipts_batches_grn_ids(
    monkeypatch: MonkeyPatch,
) -> None:
    """Large replay sets should be split into bounded request batches."""
    record_count = 26
    state_ids = [str(grn_id) for grn_id in range(1, record_count + 1)]
    tap = make_tap(
        {"bookmarks": {"open_receipts": {"grn_ids": state_ids}}}
    )
    stream = OpenReceiptsStream(cast("Any", tap))
    calls = []

    def fake_get_records(
        _stream: EasyEcomStream, context: dict[str, list[str]]
    ) -> Iterator[dict[str, Any]]:
        calls.append(context["grn_ids"])
        for grn_id in context["grn_ids"]:
            yield {
                "grn_id": grn_id,
                "grn_status_id": 5,
                "grn_status": "Completed",
            }

    monkeypatch.setattr(EasyEcomStream, "get_records", fake_get_records)

    records = list(stream.get_records(None))

    assert len(records) == record_count
    assert [len(batch) for batch in calls] == [10, 10, 6]
    assert stream.stream_state["grn_ids"] == []


def test_completed_id_stays_in_state_until_record_is_consumed(
    monkeypatch: MonkeyPatch,
) -> None:
    """A completed ID must not leave state before its final record is consumed."""
    completed_grn_id = 101
    tap = make_tap(
        {"bookmarks": {"open_receipts": {"grn_ids": [completed_grn_id]}}}
    )
    stream = OpenReceiptsStream(cast("Any", tap))

    def fake_get_records(
        _stream: EasyEcomStream, _context: dict[str, list[str]]
    ) -> Iterator[dict[str, Any]]:
        yield {"grn_id": completed_grn_id, "grn_status": "Completed"}

    monkeypatch.setattr(EasyEcomStream, "get_records", fake_get_records)

    records = iter(stream.get_records(None))
    assert next(records)["grn_id"] == completed_grn_id
    assert stream.stream_state["grn_ids"] == [str(completed_grn_id)]
    assert list(records) == []
    assert stream.stream_state["grn_ids"] == []


def test_open_receipts_runs_immediately_after_receipts(tmp_path: Path) -> None:
    """Actual SDK runtime order should make the discovery cache available."""
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"start_date": "2026-01-01T00:00:00Z"}))
    tap = TapEasyEcom(config=[config_path], validate_config=False)
    runtime_names = list(tap.streams)

    assert runtime_names.index("open_receipts") == (
        runtime_names.index("receipts") + 1
    )
