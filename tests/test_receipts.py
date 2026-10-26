# ruff: noqa: PLR2004, S101
"""Regression tests for modification-based GRN extraction and state safety."""

from __future__ import annotations

import json
from copy import deepcopy
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import parse_qs, urlparse

import pendulum
import pytest
import requests
from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.exceptions import FatalAPIError

from tap_easyecom.streams import ReceiptsStream
from tap_easyecom.tap import TapEasyEcom

if TYPE_CHECKING:
    from pathlib import Path

    from singer_sdk.streams import RESTStream, Stream


@pytest.fixture(autouse=True)
def block_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """Fail closed if a test attempts an unmocked HTTP request."""

    def reject_request(*_args: object, **_kwargs: object) -> None:
        msg = "Unexpected HTTP request in offline receipt tests"
        raise AssertionError(msg)

    monkeypatch.setattr(requests.Session, "send", reject_request)
    monkeypatch.setattr(ReceiptsStream, "authenticator", property(APIAuthenticatorBase))


def make_tap(tmp_path: Path, state: dict[str, Any] | None = None) -> TapEasyEcom:
    """Create a real SDK tap with only receipts selected and no credentials."""
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"start_date": "2026-01-01T00:00:00Z"}))
    tap = TapEasyEcom(config=[config_path], state=deepcopy(state or {}))
    for name, stream in tap.streams.items():
        stream.metadata.root.selected = name == "receipts"
    return tap


def mock_pages(
    monkeypatch: pytest.MonkeyPatch,
    stream: Stream,
    pages: list[dict[str, Any] | int],
) -> list[dict[str, list[str]]]:
    """Mock HTTP responses while exercising request preparation and pagination."""
    calls: list[dict[str, list[str]]] = []

    def send(request: requests.PreparedRequest, **_kwargs: object) -> requests.Response:
        assert request.url is not None
        calls.append(parse_qs(urlparse(request.url).query))
        page = pages[len(calls) - 1]
        response = requests.Response()
        response.status_code = page if isinstance(page, int) else 200
        response.url = request.url
        response.request = request
        response._content = json.dumps(  # noqa: SLF001
            {"message": "Synthetic request failure"} if isinstance(page, int) else page,
        ).encode()
        return response

    monkeypatch.setattr(cast("RESTStream", stream).requests_session, "send", send)
    return calls


def receipt(
    grn_id: int,
    updated_at: str,
    quantity: int = 1,
    status: str = "Completed",
) -> dict[str, Any]:
    """Build a synthetic GRN whose creation predates the modification window."""
    return {
        "grn_id": grn_id,
        "grn_created_at": "2025-12-01 09:00:00",
        "grn_updated_at": updated_at,
        "grn_status": status,
        "grn_items": [{"grn_detail_id": grn_id * 10, "received_quantity": quantity}],
    }


def messages(capsys: pytest.CaptureFixture[str]) -> list[dict[str, Any]]:
    """Read emitted Singer messages, not intermediate record objects."""
    return [json.loads(line) for line in capsys.readouterr().out.splitlines()]


def test_receipts_extracts_completed_edits_and_resumes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Old completed GRNs remain eligible after edits, across pages and runs."""
    tap = make_tap(tmp_path)
    mock_pages(
        monkeypatch,
        tap.streams["receipts"],
        [
            {
                "data": [receipt(101, "2026-01-05 13:00:00")],
                "nextUrl": "https://api.easyecom.io/Grn/V2/getGrnDetails?cursor=page-2",
            },
            {
                "data": [receipt(102, "2026-01-05 12:00:00", status="In Progress")],
                "nextUrl": None,
            },
        ],
    )
    tap.sync_all()
    first_records = [
        msg["record"] for msg in messages(capsys) if msg["type"] == "RECORD"
    ]
    assert {row["grn_id"] for row in first_records} == {101, 102}
    assert first_records[0]["grn_items"][0]["received_quantity"] == 1
    first_bookmark = tap.state["bookmarks"]["receipts"]
    assert first_bookmark["replication_key"] == "grn_updated_at"
    assert pendulum.parse(first_bookmark["replication_key_value"]) == pendulum.parse(
        "2026-01-05T13:00:00Z",
    )

    resumed = make_tap(tmp_path, tap.state)
    calls = mock_pages(
        monkeypatch,
        resumed.streams["receipts"],
        [
            {
                "data": [receipt(101, "2026-01-06 14:00:00", quantity=3)],
                "nextUrl": "https://api.easyecom.io/Grn/V2/getGrnDetails?cursor=page-2",
            },
            {
                "data": [
                    receipt(103, "2026-01-06 14:00:00"),
                    receipt(102, "2026-01-05 12:00:00", status="In Progress"),
                ],
                "nextUrl": None,
            },
        ],
    )
    resumed.sync_all()
    output = messages(capsys)
    records = [msg["record"] for msg in output if msg["type"] == "RECORD"]
    edited = next(row for row in records if row["grn_id"] == 101)
    assert edited["grn_status"] == "Completed"
    assert edited["grn_items"][0]["received_quantity"] == 3
    assert {row["grn_id"] for row in records} == {101, 102, 103}
    assert calls == [
        {"lastupdate_after": ["2026-01-05 01:00:00"]},
        {"cursor": ["page-2"], "lastupdate_after": ["2026-01-05 01:00:00"]},
    ]
    final_state = [msg["value"] for msg in output if msg["type"] == "STATE"][-1]
    bookmark = final_state["bookmarks"]["receipts"]
    assert bookmark["replication_key"] == "grn_updated_at"
    assert pendulum.parse(bookmark["replication_key_value"]) == pendulum.parse(
        "2026-01-06T14:00:00Z",
    )


def test_receipts_first_sync_honors_start_date(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A new GRN with an initial modification timestamp is extracted on first sync."""
    tap = make_tap(tmp_path)
    row = receipt(101, "2026-01-02 09:00:00", status="In Progress")
    row["grn_created_at"] = row["grn_updated_at"]
    calls = mock_pages(monkeypatch, tap.streams["receipts"], [{"data": [row]}])
    tap.sync_all()
    records = [msg["record"] for msg in messages(capsys) if msg["type"] == "RECORD"]
    assert records[0]["grn_id"] == 101
    assert records[0]["grn_updated_at"] == records[0]["grn_created_at"]
    assert calls == [{"lastupdate_after": ["2026-01-01 00:00:00"]}]


def test_receipts_legacy_bookmark_replays_from_start_date(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A creation bookmark must not skip older modifications during cutover."""
    state = {
        "bookmarks": {
            "receipts": {
                "replication_key": "grn_created_at",
                "replication_key_value": "2026-01-20 00:00:00",
            },
            "open_receipts": {"grn_ids": [101]},
            "products": {
                "replication_key": "updated_at",
                "replication_key_value": "2026-01-19 00:00:00",
            },
        }
    }
    tap = make_tap(tmp_path, state)
    calls = mock_pages(
        monkeypatch,
        tap.streams["receipts"],
        [
            {"data": [receipt(101, "2026-01-10 10:00:00")]},
        ],
    )
    tap.sync_all()
    records = [msg["record"] for msg in messages(capsys) if msg["type"] == "RECORD"]
    assert records[0]["grn_id"] == 101
    assert calls == [{"lastupdate_after": ["2026-01-01 00:00:00"]}]
    bookmark = tap.state["bookmarks"]["receipts"]
    assert bookmark["replication_key"] == "grn_updated_at"
    assert pendulum.parse(bookmark["replication_key_value"]) == pendulum.parse(
        "2026-01-10T10:00:00Z",
    )
    assert tap.state["bookmarks"]["products"] == state["bookmarks"]["products"]


def test_receipts_failed_page_preserves_durable_bookmark(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """A failed continuation page must leave the previous cursor resumable."""
    bookmark = {
        "replication_key": "grn_updated_at",
        "replication_key_value": "2026-01-05 13:00:00",
    }
    tap = make_tap(tmp_path, {"bookmarks": {"receipts": bookmark}})
    mock_pages(
        monkeypatch,
        tap.streams["receipts"],
        [
            {
                "data": [
                    receipt(101, "2026-01-06 14:00:00"),
                    receipt(102, "2026-01-06 13:00:00"),
                ],
                "nextUrl": "https://api.easyecom.io/Grn/V2/getGrnDetails?cursor=page-2",
            },
            400,
        ],
    )
    with pytest.raises(FatalAPIError):
        tap.sync_all()
    output = messages(capsys)
    assert {msg["record"]["grn_id"] for msg in output if msg["type"] == "RECORD"} == {
        101,
        102,
    }
    states = [msg["value"] for msg in output if msg["type"] == "STATE"]
    assert states
    for state in states:
        assert (
            state["bookmarks"]["receipts"]["replication_key_value"]
            == bookmark["replication_key_value"]
        )
    assert (
        tap.state["bookmarks"]["receipts"]["replication_key_value"]
        == bookmark["replication_key_value"]
    )


def test_receipts_empty_sync_keeps_bookmark(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """An empty modification window must not advance or rewind state."""
    bookmark = {
        "replication_key": "grn_updated_at",
        "replication_key_value": "2026-01-05 13:00:00",
    }
    tap = make_tap(tmp_path, {"bookmarks": {"receipts": bookmark}})
    calls = mock_pages(
        monkeypatch, tap.streams["receipts"], [{"data": "No Data Found"}]
    )
    tap.sync_all()
    output = messages(capsys)
    assert not [msg for msg in output if msg["type"] == "RECORD"]
    final_state = [msg["value"] for msg in output if msg["type"] == "STATE"][-1]
    assert final_state["bookmarks"]["receipts"] == bookmark
    assert calls == [{"lastupdate_after": ["2026-01-05 01:00:00"]}]


def test_receipts_rejects_creation_based_catalog(tmp_path: Path) -> None:
    """A stale catalog must not silently restore creation-based replication."""
    tap = make_tap(tmp_path)
    catalog = tap.catalog_dict
    entry = next(
        stream for stream in catalog["streams"] if stream["stream"] == "receipts"
    )
    entry["replication_key"] = "grn_created_at"
    stale_tap = TapEasyEcom(config=[tmp_path / "config.json"], catalog=catalog)
    with pytest.raises(ValueError, match="regenerate.*catalog"):
        stale_tap.sync_all()
