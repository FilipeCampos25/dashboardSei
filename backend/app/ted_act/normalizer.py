from __future__ import annotations

"""Normalize local and remote TED/ACT payloads into a stable internal shape."""

from dataclasses import dataclass
from typing import Any

from .process_reader import ExtractedProcessRecord


CONSOLIDATED_COLUMNS = [
    "tipo_instrumento",
    "numero_instrumento",
    "numero_processo",
    "numero_processo_consulta",
    "orgao_origem",
    "orgao_destino",
    "objeto",
    "valor",
    "data_assinatura",
    "inicio_vigencia",
    "fim_vigencia",
    "situacao",
    "fonte",
    "arquivo_origem_processo",
    "estrategia_match",
]

TED_FIELD_ALIASES = {
    "numero_instrumento": ["tx_numero_ns_termo", "tx_numero_termo", "numero_instrumento", "numero_ted"],
    "numero_processo": ["tx_num_processo_sei", "numero_processo", "processo"],
    "orgao_origem": [
        "unidade_descentralizadora",
        "orgao_origem",
        "tx_orgao_origem",
        "sigla_unidade_descentralizadora",
    ],
    "orgao_destino": [
        "unidade_descentralizada",
        "orgao_destino",
        "tx_orgao_destino",
        "sigla_unidade_descentralizada",
    ],
    "objeto": ["tx_objeto", "objeto", "descricao_objeto", "tx_descricao_objeto"],
    "valor": ["vl_total", "valor", "valor_total", "vl_global", "valor_global"],
    "data_assinatura": ["dt_assinatura_termo", "data_assinatura", "dt_assinatura"],
    "inicio_vigencia": ["dt_inicio_vigencia", "inicio_vigencia", "dt_inicio"],
    "fim_vigencia": ["dt_fim_vigencia", "fim_vigencia", "dt_fim"],
    "situacao": ["tx_situacao_termo", "situacao", "tx_situacao"],
}

ACT_FIELD_ALIASES = {
    "numero_instrumento": [
        "numero_instrumento",
        "numero",
        "tx_numero",
        "numero_act",
        "num_act",
        "instrumento",
    ],
    "numero_processo": ["numero_processo", "processo", "tx_num_processo_sei", "numeroProcesso"],
    "orgao_origem": [
        "orgao_origem",
        "unidade_concedente",
        "unidade_descentralizadora",
        "sigla_orgao_origem",
        "orgaoSuperior",
    ],
    "orgao_destino": [
        "orgao_destino",
        "unidade_recebedora",
        "unidade_descentralizada",
        "parceiro",
        "orgaoDestino",
    ],
    "objeto": ["objeto", "descricao_objeto", "tx_objeto", "objeto_acordo"],
    "valor": ["valor", "valor_total", "valor_global", "vl_global", "vl_total"],
    "data_assinatura": ["data_assinatura", "dt_assinatura", "dt_assinatura_acordo"],
    "inicio_vigencia": ["inicio_vigencia", "dt_inicio_vigencia", "dt_inicio"],
    "fim_vigencia": ["fim_vigencia", "dt_fim_vigencia", "dt_fim"],
    "situacao": ["situacao", "tx_situacao", "status"],
}


@dataclass(frozen=True)
class NormalizationResult:
    """Normalized result for a single process lookup."""

    consolidados: list[dict[str, Any]]
    nao_encontrado: dict[str, Any] | None


class TedActNormalizer:
    """Convert raw client results into consolidated TED/ACT records."""

    def normalize(
        self,
        process_record: ExtractedProcessRecord,
        ted_payload: dict[str, Any],
        act_payload: dict[str, Any],
    ) -> NormalizationResult:
        """Normalize TED and ACT payloads for one process."""
        consolidados: list[dict[str, Any]] = []
        consolidados.extend(self._normalize_ted_records(process_record, ted_payload))
        consolidados.extend(self._normalize_act_records(process_record, act_payload))

        if consolidados:
            return NormalizationResult(consolidados=consolidados, nao_encontrado=None)

        return NormalizationResult(
            consolidados=[],
            nao_encontrado=self.build_not_found_record(process_record, ted_payload, act_payload),
        )

    def build_not_found_record(
        self,
        process_record: ExtractedProcessRecord,
        ted_payload: dict[str, Any],
        act_payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Build a record for processes that produced no normalized output."""
        return {
            "numero_processo": process_record.processo_normalizado,
            "numero_processo_origem": process_record.processo_original,
            "arquivo_origem_processo": str(process_record.source_path),
            "ted_status": ted_payload.get("status"),
            "ted_error": ted_payload.get("error"),
            "ted_tentativas": ted_payload.get("attempts", []),
            "act_status": act_payload.get("status"),
            "act_error": act_payload.get("error"),
            "act_tentativas": act_payload.get("attempts", []),
        }

    def _normalize_ted_records(
        self,
        process_record: ExtractedProcessRecord,
        ted_payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Normalize TED records into the consolidated output schema."""
        records = ted_payload.get("records", [])
        if not isinstance(records, list):
            return []

        normalized: list[dict[str, Any]] = []
        for raw in records:
            if not isinstance(raw, dict):
                continue
            normalized.append(
                self._build_consolidated_record(
                    source="ted",
                    raw_record=raw,
                    aliases=TED_FIELD_ALIASES,
                    process_record=process_record,
                    client_payload=ted_payload,
                    tipo_default="TED",
                )
            )
        return normalized

    def _normalize_act_records(
        self,
        process_record: ExtractedProcessRecord,
        act_payload: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Normalize ACT records into the consolidated output schema."""
        value = act_payload.get("value")
        records = self._coerce_records(value)
        normalized: list[dict[str, Any]] = []

        for raw in records:
            if not isinstance(raw, dict):
                continue
            normalized.append(
                self._build_consolidated_record(
                    source="act",
                    raw_record=raw,
                    aliases=ACT_FIELD_ALIASES,
                    process_record=process_record,
                    client_payload=act_payload,
                    tipo_default="ACT",
                )
            )
        return normalized

    def _build_consolidated_record(
        self,
        source: str,
        raw_record: dict[str, Any],
        aliases: dict[str, list[str]],
        process_record: ExtractedProcessRecord,
        client_payload: dict[str, Any],
        tipo_default: str,
    ) -> dict[str, Any]:
        """Build one consolidated record from a raw API record."""
        return {
            "tipo_instrumento": self._first_present(raw_record, ["tipo_instrumento", "tipo", "instrumento_tipo"]) or tipo_default,
            "numero_instrumento": self._first_present(raw_record, aliases["numero_instrumento"]),
            "numero_processo": self._first_present(raw_record, aliases["numero_processo"]) or process_record.processo_normalizado,
            "numero_processo_consulta": self._resolve_query_process(process_record, client_payload),
            "orgao_origem": self._first_present(raw_record, aliases["orgao_origem"]),
            "orgao_destino": self._first_present(raw_record, aliases["orgao_destino"]),
            "objeto": self._first_present(raw_record, aliases["objeto"]),
            "valor": self._first_present(raw_record, aliases["valor"]),
            "data_assinatura": self._first_present(raw_record, aliases["data_assinatura"]),
            "inicio_vigencia": self._first_present(raw_record, aliases["inicio_vigencia"]),
            "fim_vigencia": self._first_present(raw_record, aliases["fim_vigencia"]),
            "situacao": self._first_present(raw_record, aliases["situacao"]),
            "fonte": source,
            "arquivo_origem_processo": str(process_record.source_path),
            "estrategia_match": client_payload.get("matched_strategy"),
        }

    def _resolve_query_process(
        self,
        process_record: ExtractedProcessRecord,
        client_payload: dict[str, Any],
    ) -> str:
        """Resolve which process string actually matched or was last attempted."""
        strategy = client_payload.get("matched_strategy")
        if strategy == "original":
            return process_record.processo_original
        if strategy == "normalized":
            return process_record.processo_normalizado

        attempts = client_payload.get("attempts", [])
        if attempts:
            attempt_process = attempts[-1].get("processo")
            if attempt_process:
                return str(attempt_process)
        return process_record.processo_normalizado

    def _coerce_records(self, value: Any) -> list[Any]:
        """Convert a raw ACT payload into a list of records."""
        if value is None:
            return []
        if isinstance(value, list):
            return value
        if isinstance(value, dict):
            return [value]
        return []

    def _first_present(self, raw_record: dict[str, Any], aliases: list[str]) -> Any:
        """Return the first non-empty value found in the alias list."""
        for alias in aliases:
            if alias in raw_record and raw_record.get(alias) not in ("", None):
                return raw_record.get(alias)
        return None
