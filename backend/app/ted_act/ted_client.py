from __future__ import annotations

"""Client for querying TED data by process number."""

import argparse
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

import requests

from .config import TedActModuleConfig
from .config import load_ted_act_config


logger = logging.getLogger(__name__)
PROCESS_PATTERN = re.compile(r"\b\d{5}\.\d{6}/\d{4}-\d{2}\b")


@dataclass
class TedClient:
    """Query the TED API with isolated error handling."""

    config: TedActModuleConfig
    session: requests.Session = field(default_factory=requests.Session)
    default_headers: dict[str, str] = field(init=False)

    def __post_init__(self) -> None:
        self.default_headers = self.build_headers()

    def build_headers(self) -> dict[str, str]:
        """Return headers for TED requests."""
        headers = {"Accept": "application/json"}
        if self.config.resolved_ted_api_key:
            headers["Authorization"] = f"Bearer {self.config.resolved_ted_api_key}"
        return headers

    def fetch_by_process(self, processo: str) -> dict[str, Any]:
        """Query TED by process number using original and normalized variants."""
        endpoint = self.config.ted_endpoint_url
        if not endpoint:
            logger.warning("TED client not configured: missing endpoint.")
            return {
                "client": "ted",
                "status": "not_configured",
                "processo_original": processo,
                "processo_normalizado": self.normalize_processo(processo),
                "endpoint": None,
                "matched_strategy": None,
                "raw_response": None,
                "records": [],
                "attempts": [],
                "error": "ted_endpoint_missing",
            }

        attempts: list[dict[str, Any]] = []
        last_error: str | None = None
        processo_normalizado = self.normalize_processo(processo)

        for strategy_name, candidate in self._build_candidates(processo):
            params = {
                self.config.ted_process_query_param: f"eq.{candidate}",
            }
            try:
                response = self.session.get(
                    endpoint,
                    params=params,
                    headers=self.default_headers,
                    timeout=self.config.timeout_seconds,
                    verify=self.config.verify_ssl,
                )
                response.raise_for_status()
                payload = response.json()
                records = payload if isinstance(payload, list) else []
                attempts.append(
                    {
                        "strategy": strategy_name,
                        "processo": candidate,
                        "url": response.url,
                        "status_code": response.status_code,
                        "record_count": len(records),
                    }
                )
                logger.debug(
                    "TED query strategy=%s processo=%s status=%s records=%d",
                    strategy_name,
                    candidate,
                    response.status_code,
                    len(records),
                )
                if records:
                    return {
                        "client": "ted",
                        "status": "ok",
                        "processo_original": processo,
                        "processo_normalizado": processo_normalizado,
                        "endpoint": endpoint,
                        "matched_strategy": strategy_name,
                        "raw_response": payload,
                        "records": records,
                        "attempts": attempts,
                        "error": None,
                    }
            except requests.RequestException as exc:
                last_error = str(exc)
                logger.warning(
                    "TED request failed for processo=%s strategy=%s (%s)",
                    candidate,
                    strategy_name,
                    exc,
                )
                attempts.append(
                    {
                        "strategy": strategy_name,
                        "processo": candidate,
                        "url": endpoint,
                        "status_code": None,
                        "record_count": None,
                        "error": str(exc),
                    }
                )
            except ValueError as exc:
                last_error = f"invalid_json: {exc}"
                logger.warning(
                    "TED response JSON parsing failed for processo=%s strategy=%s (%s)",
                    candidate,
                    strategy_name,
                    exc,
                )
                attempts.append(
                    {
                        "strategy": strategy_name,
                        "processo": candidate,
                        "url": endpoint,
                        "status_code": None,
                        "record_count": None,
                        "error": f"invalid_json: {exc}",
                    }
                )

        if any(attempt.get("status_code") == 200 for attempt in attempts):
            return {
                "client": "ted",
                "status": "empty",
                "processo_original": processo,
                "processo_normalizado": processo_normalizado,
                "endpoint": endpoint,
                "matched_strategy": None,
                "raw_response": [],
                "records": [],
                "attempts": attempts,
                "error": None,
            }

        return {
            "client": "ted",
            "status": "error",
            "processo_original": processo,
            "processo_normalizado": processo_normalizado,
            "endpoint": endpoint,
            "matched_strategy": None,
            "raw_response": None,
            "records": [],
            "attempts": attempts,
            "error": last_error,
        }

    def _build_candidates(self, processo: str) -> list[tuple[str, str]]:
        """Return unique query candidates in priority order."""
        ordered: list[tuple[str, str]] = []
        seen: set[str] = set()
        for strategy_name, value in [
            ("original", (processo or "").strip()),
            ("normalized", self.normalize_processo(processo)),
        ]:
            if not value or value in seen:
                continue
            seen.add(value)
            ordered.append((strategy_name, value))
        return ordered

    def normalize_processo(self, processo: str) -> str:
        """Normalize a process number to the canonical SEI mask when possible."""
        raw = (processo or "").strip()
        match = PROCESS_PATTERN.search(raw)
        if match:
            return match.group(0)
        digits = re.sub(r"\D", "", raw)
        if len(digits) != 17:
            return raw
        return f"{digits[:5]}.{digits[5:11]}/{digits[11:15]}-{digits[15:17]}"


def main() -> None:
    """Run the TED client in isolation for manual testing."""
    parser = argparse.ArgumentParser(description="TED API client test")
    parser.add_argument("processo", help="Numero do processo a consultar")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    client = TedClient(load_ted_act_config())
    print(json.dumps(client.fetch_by_process(args.processo), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
