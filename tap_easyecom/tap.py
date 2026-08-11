"""EasyEcom tap class."""

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_easyecom.streams import (
    ProductsStream,
    ProductCompositionsStream,
    SuppliersStream,
    SellOrdersStream,
    BuyOrdersStream,
    ReceiptsStream,
    OpenReceiptsStream,
    ReturnsStream,
)

STREAM_TYPES = [
    ProductsStream,
    ProductCompositionsStream,
    SuppliersStream,
    SellOrdersStream,
    BuyOrdersStream,
    ReceiptsStream,
    OpenReceiptsStream,
    ReturnsStream,
]


class TapEasyEcom(Tap):
    """EasyEcom tap class."""

    name = "tap-easyecom"

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.open_grn_ids_cache = set()
        super().__init__(config, catalog, state, parse_env_config, validate_config)
        self.config_file = config[0]
        self._place_open_receipts_after_receipts()

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "start_date",
            th.DateTimeType,
        ),
        th.Property("x_api_key", th.StringType),
        th.Property("x-api-key", th.StringType),
    ).to_dict()

    def discover_streams(self):
        return [stream(self) for stream in STREAM_TYPES]

    def _place_open_receipts_after_receipts(self) -> None:
        """Reorder the loaded stream mapping for the runtime cache handoff."""
        streams = self.streams
        if "receipts" not in streams or "open_receipts" not in streams:
            return

        ordered_streams = {}
        for name, stream in streams.items():
            if name == "open_receipts":
                continue
            ordered_streams[name] = stream
            if name == "receipts":
                ordered_streams["open_receipts"] = streams["open_receipts"]
        self._streams = ordered_streams


if __name__ == "__main__":
    TapEasyEcom.cli()
