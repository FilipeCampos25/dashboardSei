from __future__ import annotations

"""Client for querying ACT data by process number."""

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
class ActClient:
    """Query the ACT API with isolated error handling."""

    config: TedActModuleConfig
    session: requests.Session = field(default_factory=requests.Session)
    default_headers: dict[str, str] = field(init=False)

    def __post_init__(self) -> None:
        self.default_headers = self.build_headers()

    def build_headers(self) -> dict[str, str]:
        """Return headers for ACT requests."""
        headers = {"Accept": "application/json"}
        if self.config.resolved_act_api_key:
            headers[self.config.act_api_key_header] = self.config.resolved_act_api_key
        return headers

    def fetch_by_process(self, processo: str, hints: dict[str, Any] | None = None) -> dict[str, Any]:
        """Query ACT using instrument and partner hints, then filter by process."""
        endpoint = self.config.act_endpoint_url
        processo_normalizado = self.normalize_processo(processo)
        hints = hints or {}
        if not endpoint:
            logger.warning("ACT client not configured: missing endpoint.")
            return {
                "client": "act",
                "status": "not_configured",
                "processo_original": processo,
                "processo_normalizado": processo_normalizado,
                "endpoint": None,
                "matched_strategy": None,
                "raw_response": None,
                "value": None,
                "attempts": [],
                "error": "act_endpoint_missing",
            }

        if not self.config.resolved_act_api_key:
            logger.warning("ACT client not configured: missing API key.")
            return {
                "client": "act",
                "status": "not_configured",
                "processo_original": processo,
                "processo_normalizado": processo_normalizado,
                "endpoint": endpoint,
                "matched_strategy": None,
                "raw_response": None,
                "value": None,
                "attempts": [],
                "error": "act_api_key_missing",
            }

        attempts: list[dict[str, Any]] = []
        last_error: str | None = None
        candidates = self._build_candidates(hints)

        if not candidates:
            logger.info(
                "ACT query skipped for processo=%s: no instrument number or partner hint available.",
                processo_normalizado,
            )
            return {
                "client": "act",
                "status": "empty",
                "processo_original": processo,
                "processo_normalizado": processo_normalizado,
                "endpoint": endpoint,
                "matched_strategy": None,
                "raw_response": None,
                "value": None,
                "attempts": [],
                "error": "act_no_query_hints",
            }

        for strategy_name, params in candidates:
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
                records = self._coerce_records(payload)
                filtered_records = self._filter_records(records, processo_normalizado, hints)
                is_empty = not filtered_records
                attempts.append(
                    {
                        "strategy": strategy_name,
                        "params": params,
                        "url": response.url,
                        "status_code": response.status_code,
                        "raw_count": len(records),
                        "filtered_count": len(filtered_records),
                        "is_empty": is_empty,
                    }
                )
                logger.debug(
                    "ACT query strategy=%s status=%s raw=%s filtered=%s",
                    strategy_name,
                    response.status_code,
                    len(records),
                    len(filtered_records),
                )
                if not is_empty:
                    return {
                        "client": "act",
                        "status": "ok",
                        "processo_original": processo,
                        "processo_normalizado": processo_normalizado,
                        "endpoint": endpoint,
                        "matched_strategy": strategy_name,
                        "raw_response": payload,
                        "value": filtered_records,
                        "attempts": attempts,
                        "error": None,
                    }
            except requests.RequestException as exc:
                last_error = str(exc)
                logger.warning(
                    "ACT request failed for processo=%s strategy=%s (%s)",
                    processo_normalizado,
                    strategy_name,
                    exc,
                )
                attempts.append(
                    {
                        "strategy": strategy_name,
                        "params": params,
                        "url": endpoint,
                        "status_code": None,
                        "is_empty": None,
                        "error": str(exc),
                    }
                )
            except ValueError as exc:
                last_error = f"invalid_json: {exc}"
                logger.warning(
                    "ACT response JSON parsing failed for processo=%s strategy=%s (%s)",
                    processo_normalizado,
                    strategy_name,
                    exc,
                )
                attempts.append(
                    {
                        "strategy": strategy_name,
                        "params": params,
                        "url": endpoint,
                        "status_code": None,
                        "is_empty": None,
                        "error": f"invalid_json: {exc}",
                    }
                )

        if any(attempt.get("status_code") == 200 for attempt in attempts):
            return {
                "client": "act",
                "status": "empty",
                "processo_original": processo,
                "processo_normalizado": processo_normalizado,
                "endpoint": endpoint,
                "matched_strategy": None,
                "raw_response": None,
                "value": None,
                "attempts": attempts,
                "error": None,
            }

        return {
            "client": "act",
            "status": "error",
            "processo_original": processo,
            "processo_normalizado": processo_normalizado,
            "endpoint": endpoint,
            "matched_strategy": None,
            "raw_response": None,
            "value": None,
            "attempts": attempts,
            "error": last_error,
        }

    def _build_candidates(self, hints: dict[str, Any]) -> list[tuple[str, dict[str, str]]]:
        """Return ACT query strategies in priority order."""
        ordered: list[tuple[str, dict[str, str]]] = []
        seen: set[tuple[tuple[str, str], ...]] = set()
        candidates = [
            ("instrument_number", self._build_numero_params(hints)),
            ("partner_name", self._build_convenente_params(hints)),
        ]
        for strategy_name, params in candidates:
            if not params:
                continue
            signature = tuple(sorted((str(k), str(v)) for k, v in params.items()))
            if signature in seen:
                continue
            seen.add(signature)
            ordered.append((strategy_name, params))
        return ordered

    def _build_numero_params(self, hints: dict[str, Any]) -> dict[str, str]:
        """Build params using the instrument number hint."""
        numero = str(hints.get("numero_instrumento", "") or "").strip()
        if not numero:
            return {}
        return {
            self.config.act_process_query_param: numero,
            "pagina": "1",
        }

    def _build_convenente_params(self, hints: dict[str, Any]) -> dict[str, str]:
        """Build params using the partner/convenente hint."""
        parceiro = str(hints.get("parceiro", "") or "").strip()
        if not parceiro:
            return {}
        return {
            "convenente": parceiro,
            "pagina": "1",
        }

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

    def _coerce_records(self, value: Any) -> list[Any]:
        """Convert the ACT payload into a list of records."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            return [value]
        return []

    def _filter_records(
        self,
        records: list[Any],
        processo_normalizado: str,
        hints: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Filter convenio records using process and local hints."""
        filtered: list[dict[str, Any]] = []
        numero_hint = str(hints.get("numero_instrumento", "") or "").strip().lower()
        tipo_hint = str(hints.get("tipo_instrumento", "") or "").strip().lower()

        for raw in records:
            if not isinstance(raw, dict):
                continue
            haystack = self._stringify(raw).lower()
            process_match = processo_normalizado.lower() in haystack
            numero_match = bool(numero_hint) and numero_hint in haystack
            tipo_match = bool(tipo_hint) and tipo_hint in haystack
            if process_match or numero_match or (numero_match and tipo_match):
                filtered.append(self._promote_fields(raw, processo_normalizado))
        return filtered

    def _promote_fields(self, raw: dict[str, Any], processo_normalizado: str) -> dict[str, Any]:
        """Promote nested convenio fields to flat aliases expected by the normalizer."""
        promoted = dict(raw)
        dim_convenio = raw.get("dimConvenio", {}) if isinstance(raw.get("dimConvenio"), dict) else {}
        convenente = raw.get("convenente", {}) if isinstance(raw.get("convenente"), dict) else {}

        if "numero_instrumento" not in promoted and dim_convenio.get("numero") not in ("", None):
            promoted["numero_instrumento"] = dim_convenio.get("numero")
        if "objeto" not in promoted and dim_convenio.get("objeto") not in ("", None):
            promoted["objeto"] = dim_convenio.get("objeto")
        if "orgao_destino" not in promoted and convenente.get("nome") not in ("", None):
            promoted["orgao_destino"] = convenente.get("nome")
        if "situacao" not in promoted and raw.get("situacao") not in ("", None):
            promoted["situacao"] = raw.get("situacao")
        if "inicio_vigencia" not in promoted and raw.get("dataInicioVigencia") not in ("", None):
            promoted["inicio_vigencia"] = raw.get("dataInicioVigencia")
        if "fim_vigencia" not in promoted and raw.get("dataFinalVigencia") not in ("", None):
            promoted["fim_vigencia"] = raw.get("dataFinalVigencia")
        if "data_assinatura" not in promoted and raw.get("dataPublicacao") not in ("", None):
            promoted["data_assinatura"] = raw.get("dataPublicacao")
        promoted.setdefault("numero_processo", processo_normalizado)
        return promoted

    def _stringify(self, value: Any) -> str:
        """Collapse nested ACT structures into a searchable string."""
        if isinstance(value, dict):
            return " ".join(self._stringify(item) for item in value.values())
        if isinstance(value, list):
            return " ".join(self._stringify(item) for item in value)
        return str(value or "")


def main() -> None:
    """Run the ACT client in isolation for manual testing."""
    parser = argparse.ArgumentParser(description="ACT API client test")
    parser.add_argument("processo", help="Numero do processo a consultar")
    parser.add_argument(
        "--numero-instrumento",
        dest="numero_instrumento",
        default="",
        help="Numero do ACT/acordo para consulta manual",
    )
    parser.add_argument(
        "--parceiro",
        default="",
        help="Nome do parceiro/convenente para consulta manual",
    )
    parser.add_argument(
        "--tipo-instrumento",
        dest="tipo_instrumento",
        default="",
        help="Tipo esperado do instrumento, ex.: ACT, ACORDO, MEMORANDO",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)
    client = ActClient(load_ted_act_config())
    result = client.fetch_by_process(
        args.processo,
        hints={
            "numero_instrumento": args.numero_instrumento,
            "parceiro": args.parceiro,
            "tipo_instrumento": args.tipo_instrumento,
        },
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
