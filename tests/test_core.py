# ruff: noqa: S101
"""Offline tests of the tap's public discovery contract."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from click.testing import CliRunner

from tap_easyecom.tap import TapEasyEcom

if TYPE_CHECKING:
    from pathlib import Path


def test_discovery_catalog(tmp_path: Path) -> None:
    """Expose modified GRNs without removing unrelated streams from discovery."""
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps({"start_date": "2026-01-01T00:00:00Z"}))
    result = CliRunner().invoke(
        TapEasyEcom.cli,
        ["--config", str(config_path), "--discover"],
    )
    assert result.exit_code == 0, result.output
    catalog = json.loads(result.stdout)
    streams = {stream["stream"]: stream for stream in catalog["streams"]}
    assert set(streams) == {
        "products",
        "product_compositions",
        "suppliers",
        "sell_orders",
        "buy_orders",
        "receipts",
        "returns",
    }
    receipts = streams["receipts"]
    assert receipts["replication_key"] == "grn_updated_at"
    assert receipts["replication_method"] == "INCREMENTAL"
    assert receipts["key_properties"] == ["grn_id"]
    properties = receipts["schema"]["properties"]
    assert properties["grn_created_at"]["format"] == "date-time"
    assert properties["grn_updated_at"]["format"] == "date-time"
