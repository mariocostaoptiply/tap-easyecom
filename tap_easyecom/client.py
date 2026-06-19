"""REST client handling, including EasyEcomStream base class."""

from __future__ import annotations

from typing import Any, Callable, ClassVar, Iterable, Optional, cast
from singer_sdk.exceptions import RetriableAPIError
from urllib.parse import urlparse, parse_qs
from functools import cached_property
import singer
from singer import StateMessage
from singer_sdk.streams import RESTStream

from tap_easyecom.auth import BearerTokenAuthenticator
from pendulum.parser import parse
import backoff
import requests


class EasyEcomStream(RESTStream):
    """EasyEcom stream class."""

    records_jsonpath = "$.data[*]"
    page_size = None
    additional_params: ClassVar[dict[str, Any]] = {}
    date_filter_param = "updated_after"

    def get_next_page_token(self, response, previous_token):
        """Return a token for identifying next page or None if no more pages."""
        res_json = response.json()
        next_url = res_json.get("nextUrl")

        if not next_url and isinstance(res_json.get("data", {}), dict):
            next_url = res_json.get("data", {}).get("nextUrl")

        if next_url:
            return parse_qs(urlparse(next_url).query)["cursor"]

        return None

    @property
    def url_base(self) -> str:
        return "https://api.easyecom.io"

    @cached_property
    def authenticator(self) -> BearerTokenAuthenticator:
        return BearerTokenAuthenticator(
            self,
            getattr(self._tap, "config_file", None),
            f"{self.url_base}/access/token",
        )

    @property
    def http_headers(self) -> dict:
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")

        # EasyEcom appears to apply a stricter API Gateway rate limit when
        # x-api-key is present on data requests. Prefer bearer-token auth only,
        # and add x-api-key later only if the endpoint rejects the request.
        return headers

    @property
    def api_key(self) -> str:
        return self.config.get("x_api_key") or self.config.get("x-api-key") or ""

    def _request_with_api_key(
        self, prepared_request: requests.PreparedRequest
    ) -> requests.PreparedRequest:
        api_key = self.api_key
        if not api_key:
            return prepared_request

        retry_request = prepared_request.copy()
        retry_request.headers["x-api-key"] = api_key
        return retry_request

    def _request_without_api_key(
        self, prepared_request: requests.PreparedRequest
    ) -> requests.PreparedRequest:
        retry_request = prepared_request.copy()
        retry_request.headers.pop("x-api-key", None)
        return retry_request

    def _send_prepared_request(
        self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:
        response = self.requests_session.send(prepared_request, timeout=self.timeout)
        if self._LOG_REQUEST_METRICS:
            extra_tags = {}
            if self._LOG_REQUEST_METRIC_URLS:
                extra_tags["url"] = prepared_request.path_url
            self._write_request_duration_log(
                endpoint=self.path,
                response=response,
                context=context,
                extra_tags=extra_tags,
            )
        return response

    def _request(
        self, prepared_request: requests.PreparedRequest, context: Optional[dict]
    ) -> requests.Response:
        sent_request = prepared_request
        response = self._send_prepared_request(sent_request, context)

        if response.status_code == 403 and self.api_key:
            self.logger.info(
                "Request returned 403 without x-api-key; retrying once with x-api-key."
            )
            sent_request = self._request_with_api_key(prepared_request)
            response = self._send_prepared_request(sent_request, context)

        if response.status_code == 429 and "x-api-key" in sent_request.headers:
            self.logger.info(
                "Request returned 429 with x-api-key; retrying once without x-api-key."
            )
            response = self._send_prepared_request(
                self._request_without_api_key(sent_request), context
            )

        self.validate_response(response)
        return response

    def get_starting_time(self, context):
        start_date = self.config.get("start_date")
        if start_date:
            start_date = cast("Any", parse(start_date))
        rep_key = self.get_starting_timestamp(context)
        return rep_key or start_date

    def get_url_params(self, context, next_page_token):
        params: dict = {}
        if next_page_token:
            params["cursor"] = next_page_token
        if self.page_size:
            params["limit"] = self.page_size
        if hasattr(self, "additional_params"):
            params.update(self.additional_params)
        if self.replication_key:
            start_date = self.get_starting_time(context)
            date_filter = self.date_filter_param
            params[date_filter] = start_date.strftime("%Y-%m-%d %H:%M:%S")
        return params

    def _write_state_message(self) -> None:
        """Write out a STATE message with the latest state."""
        tap_state = self.tap_state

        if tap_state and tap_state.get("bookmarks"):
            for stream_name in (tap_state.get("bookmarks") or {}).keys():
                if stream_name in [
                    "gl_entries_dimensions",
                ] and tap_state["bookmarks"][stream_name].get("partitions"):
                    tap_state["bookmarks"][stream_name] = {"partitions": []}

        singer.write_message(StateMessage(value=tap_state))

    def request_decorator(self, func: Callable) -> Callable:
        decorator: Callable = backoff.on_exception(
            self.backoff_wait_generator,  # type: ignore[arg-type]
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
            ),
            max_tries=10,
            on_backoff=self.backoff_handler,  # type: ignore[arg-type]
        )(func)
        return decorator

    def post_process(self, row: dict, context: Optional[dict] = None) -> dict:
        """Convert string numbers to float and handle NA values."""
        for key, value in row.items():
            # Handle actual null values
            if value is None:
                continue  # Already None, no need to change

            if isinstance(value, str):
                # Handle NA values
                if value.upper() in ["NA", "N/A", "NULL", "NONE", ""]:
                    row[key] = None
                    continue

                # Convert string numbers to float
                field_types = self.schema.get("properties", {}).get(key, {}).get("type")
                if field_types and "number" in field_types:
                    try:
                        row[key] = float(value)
                    except ValueError:
                        self.logger.debug(f"Parsing {key}={value} failed")
                        raise ValueError(f"Parsing {key}={value} failed")
        return row

    def parse_response(self, response) -> Iterable[dict]:
        if response.json().get("data") == "No Data Found":
            yield from []
        else:
            yield from super().parse_response(response)
