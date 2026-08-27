from __future__ import annotations

import json
import re
import shutil
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from app.config import get_settings
from app.output import csv_writer
from app.services.contract_adapters import (
    V2_SCHEMA_VERSION,
    adapt_legacy_record,
    v2_sidecar_path,
    write_v2_sidecar,
)
from app.services.field_states import FieldResult, FieldState
from app.services.gold_contracts import EvidenceLocation, FieldEvidence, SourceKind
from app.services.normalization_contract import (
    CONFIDENCE_HIGH,
    CONFIDENCE_LOW,
    CONFIDENCE_MEDIUM,
    SOURCE_DERIVED,
    SOURCE_DOCUMENT_METADATA,
    SOURCE_DOCUMENT_TEXT,
    SOURCE_DOCUMENT_TITLE,
    SOURCE_MISSING,
    DocumentIdentity,
    build_document_contract,
    make_field,
    make_missing_field,
)
from app.services.act_process_affinity import assess_act_process_affinity

DOC_CLASS_ACT_FINAL = "act_final"
DOC_CLASS_MEMORANDO = "memorando"
DOC_CLASS_OFICIO = "oficio"
DOC_CLASS_DESPACHO = "despacho"
DOC_CLASS_INFORMACAO_TECNICA = "informacao_tecnica"
DOC_CLASS_NOTA_TECNICA = "nota_tecnica"
DOC_CLASS_DOCUMENTO_ADMINISTRATIVO_RELACIONADO = "documento_administrativo_relacionado"
DOC_CLASS_TED = "ted"
DOC_CLASS_EXTRATO = "extrato"
DOC_CLASS_MINUTA = "minuta"
DOC_CLASS_TERMO_ADITIVO = "termo_aditivo"
DOC_CLASS_TERMO_ADESAO = "termo_adesao"
DOC_CLASS_STUB = "stub"
DOC_CLASS_EMAIL_OUTRO = "email_outro"

RESOLVED_TYPE_ACT = "act"
RESOLVED_TYPE_MEMORANDO = "memorando"
RESOLVED_TYPE_OFICIO = "oficio"
RESOLVED_TYPE_DESPACHO = "despacho"
RESOLVED_TYPE_INFORMACAO_TECNICA = "informacao_tecnica"
RESOLVED_TYPE_NOTA_TECNICA = "nota_tecnica"
RESOLVED_TYPE_DOCUMENTO_ADMINISTRATIVO_RELACIONADO = "documento_administrativo_relacionado"
RESOLVED_TYPE_TED = "termo_execucao_descentralizada"
RESOLVED_TYPE_ACT_RELATED = "act_relacionado"

SNAPSHOT_PREFIX_ACT = "acordo_cooperacao_tecnica"
SNAPSHOT_PREFIX_MEMORANDO = "memorando_entendimentos"
SNAPSHOT_PREFIX_TED = "termo_execucao_descentralizada"

DOC_CLASS_PRIORITY = {
    DOC_CLASS_ACT_FINAL: 100,
    DOC_CLASS_MEMORANDO: 80,
    DOC_CLASS_OFICIO: 75,
    DOC_CLASS_DESPACHO: 75,
    DOC_CLASS_INFORMACAO_TECNICA: 75,
    DOC_CLASS_NOTA_TECNICA: 75,
    DOC_CLASS_DOCUMENTO_ADMINISTRATIVO_RELACIONADO: 60,
    DOC_CLASS_TED: 80,
    DOC_CLASS_EXTRATO: 30,
    DOC_CLASS_MINUTA: 20,
    DOC_CLASS_TERMO_ADITIVO: 20,
    DOC_CLASS_TERMO_ADESAO: 20,
    DOC_CLASS_STUB: 10,
    DOC_CLASS_EMAIL_OUTRO: 0,
}

INVALID_TAIL_MARKERS = (
    "documento assinado eletronicamente",
    "a autenticidade do documento pode ser conferida",
    "codigo verificador",
    "codigo crc",
    "criado por ",
)

EMAIL_MARKERS = ("assunto:", "para:", "de:", "enviado:", "enviada:", "cc:", "cco:")

DOC_CLASS_RESOLVED_TYPE = {
    DOC_CLASS_ACT_FINAL: RESOLVED_TYPE_ACT,
    DOC_CLASS_MEMORANDO: RESOLVED_TYPE_MEMORANDO,
    DOC_CLASS_OFICIO: RESOLVED_TYPE_OFICIO,
    DOC_CLASS_DESPACHO: RESOLVED_TYPE_DESPACHO,
    DOC_CLASS_INFORMACAO_TECNICA: RESOLVED_TYPE_INFORMACAO_TECNICA,
    DOC_CLASS_NOTA_TECNICA: RESOLVED_TYPE_NOTA_TECNICA,
    DOC_CLASS_DOCUMENTO_ADMINISTRATIVO_RELACIONADO: RESOLVED_TYPE_DOCUMENTO_ADMINISTRATIVO_RELACIONADO,
    DOC_CLASS_TED: RESOLVED_TYPE_TED,
    DOC_CLASS_EXTRATO: RESOLVED_TYPE_ACT_RELATED,
    DOC_CLASS_MINUTA: RESOLVED_TYPE_ACT_RELATED,
    DOC_CLASS_TERMO_ADITIVO: RESOLVED_TYPE_ACT_RELATED,
    DOC_CLASS_TERMO_ADESAO: RESOLVED_TYPE_ACT_RELATED,
    DOC_CLASS_STUB: RESOLVED_TYPE_ACT_RELATED,
    DOC_CLASS_EMAIL_OUTRO: RESOLVED_TYPE_ACT_RELATED,
}

DOC_CLASS_SNAPSHOT_PREFIX = {
    DOC_CLASS_ACT_FINAL: SNAPSHOT_PREFIX_ACT,
    DOC_CLASS_MEMORANDO: SNAPSHOT_PREFIX_MEMORANDO,
    DOC_CLASS_OFICIO: SNAPSHOT_PREFIX_MEMORANDO,
    DOC_CLASS_DESPACHO: SNAPSHOT_PREFIX_MEMORANDO,
    DOC_CLASS_INFORMACAO_TECNICA: SNAPSHOT_PREFIX_MEMORANDO,
    DOC_CLASS_NOTA_TECNICA: SNAPSHOT_PREFIX_MEMORANDO,
    DOC_CLASS_DOCUMENTO_ADMINISTRATIVO_RELACIONADO: SNAPSHOT_PREFIX_MEMORANDO,
    DOC_CLASS_TED: SNAPSHOT_PREFIX_TED,
    DOC_CLASS_EXTRATO: SNAPSHOT_PREFIX_ACT,
    DOC_CLASS_MINUTA: SNAPSHOT_PREFIX_ACT,
    DOC_CLASS_TERMO_ADITIVO: SNAPSHOT_PREFIX_ACT,
    DOC_CLASS_TERMO_ADESAO: SNAPSHOT_PREFIX_ACT,
    DOC_CLASS_STUB: SNAPSHOT_PREFIX_ACT,
    DOC_CLASS_EMAIL_OUTRO: SNAPSHOT_PREFIX_ACT,
}

REQUESTED_TYPE_TO_PREFIX = {
    "act": SNAPSHOT_PREFIX_ACT,
    "memorando": SNAPSHOT_PREFIX_MEMORANDO,
    "ted": SNAPSHOT_PREFIX_TED,
}

VALIDATION_STATUS_VALID = "valid_for_requested_type"
VALIDATION_STATUS_RELATED = "related_but_not_requested"
VALIDATION_STATUS_REJECTED = "rejected_snapshot"

PUBLICATION_STATUS_GOLD = "published_gold"
PUBLICATION_STATUS_SILVER = "retained_silver"

HEADER_SCAN_CHARS = 1800
OPENING_SCAN_CHARS = 4200
LEAD_SCAN_CHARS = 350
PROCESS_SCAN_CHARS = 12000
SECTION_WINDOW_CHARS = 2200

DATE_PATTERN = r"(\d{1,2}(?:[./-]\d{1,2}[./-]\d{4}|\s+de\s+[A-Za-z\u00c0-\u00ff]+\s+de\s+\d{4}))"
PROCESS_PATTERN = r"[0-9]{5}\.[0-9]{6}/[0-9]{4}-[0-9]{2}"
SECTION_STOP_PATTERN = (
    r"(?:\n\s*(?:\d+\s*[.)-]\s*)?CL[\u00c1A]USULA\b"
    r"|\n\s*SUBCL[\u00c1A]USULA\b"
    r"|\n\s*REFER[\u00caE]NCIA:\b"
    r"|\bDocumento assinado eletronicamente\b)"
)

CONTRACTUAL_MARKERS = (
    "que entre si celebram",
    "resolvem celebrar",
    "uniao, representada",
    "participe 1",
    "participes",
    "clausula primeira",
)

TREE_PENALTY_MARKERS = (
    "anexo",
    "minuta",
    "publicacao",
    "extrato",
    "reuniao",
    "alterado",
    "plano de trabalho",
    " pt ",
)

PUBLICATION_CONTEXT_MARKERS = (
    "extrato",
    "acordo",
    "instrumento",
    "termo",
    "ajuste",
    "publicacao do acordo",
    "publicacao do instrumento",
    "publicacao do termo",
    "publicacao do presente",
    "publicacao deste",
)

NOMINATION_PUBLICATION_MARKERS = (
    "nomeado",
    "nomeada",
    "nomeacao",
    "portaria",
    "representado",
    "representada",
    "ato de nomeacao",
    "boletim de comunicacoes administrativas",
)

HEADER_REJECTION_MARKERS = {
    "minuta": (DOC_CLASS_MINUTA, "cabecalho_minuta"),
    "extrato": (DOC_CLASS_EXTRATO, "cabecalho_extrato"),
    "termo de adesao": (DOC_CLASS_TERMO_ADESAO, "cabecalho_termo_adesao"),
    "termo aditivo": (DOC_CLASS_TERMO_ADITIVO, "cabecalho_termo_aditivo"),
    "proposta de termo aditivo": (DOC_CLASS_TERMO_ADITIVO, "cabecalho_proposta_termo_aditivo"),
    "memorando de entendimentos": (DOC_CLASS_MEMORANDO, "cabecalho_memorando"),
    "memorando": (DOC_CLASS_MEMORANDO, "cabecalho_memorando"),
    "oficio": (DOC_CLASS_OFICIO, "cabecalho_oficio"),
    "despacho": (DOC_CLASS_DESPACHO, "cabecalho_despacho"),
    "informacao tecnica": (DOC_CLASS_INFORMACAO_TECNICA, "cabecalho_informacao_tecnica"),
    "nota tecnica": (DOC_CLASS_NOTA_TECNICA, "cabecalho_nota_tecnica"),
    "termo de execucao descentralizada": (DOC_CLASS_TED, "cabecalho_ted"),
    "portaria": (DOC_CLASS_EMAIL_OUTRO, "cabecalho_portaria"),
    "publicacao": (DOC_CLASS_EMAIL_OUTRO, "cabecalho_publicacao"),
    "e-mail": (DOC_CLASS_EMAIL_OUTRO, "cabecalho_email"),
    "email": (DOC_CLASS_EMAIL_OUTRO, "cabecalho_email"),
    "plano de trabalho": (DOC_CLASS_EMAIL_OUTRO, "cabecalho_plano_trabalho"),
    "reuniao": (DOC_CLASS_EMAIL_OUTRO, "cabecalho_reuniao"),
    "convenio": (DOC_CLASS_EMAIL_OUTRO, "cabecalho_convenio"),
}

ADMINISTRATIVE_DOC_CLASSES = (
    DOC_CLASS_MEMORANDO,
    DOC_CLASS_OFICIO,
    DOC_CLASS_DESPACHO,
    DOC_CLASS_INFORMACAO_TECNICA,
    DOC_CLASS_NOTA_TECNICA,
    DOC_CLASS_DOCUMENTO_ADMINISTRATIVO_RELACIONADO,
)

ADMINISTRATIVE_RELATED_MARKERS = (
    "encaminhamento",
    "encaminha",
    "encaminho",
    "encaminhamos",
    "solicitacao",
    "solicita",
    "solicito",
    "solicitamos",
)

ADMINISTRATIVE_HEADER_MARKERS = {
    "memorando de entendimentos": (DOC_CLASS_MEMORANDO, "cabecalho_memorando"),
    "memorando": (DOC_CLASS_MEMORANDO, "cabecalho_memorando"),
    "oficio": (DOC_CLASS_OFICIO, "cabecalho_oficio"),
    "despacho": (DOC_CLASS_DESPACHO, "cabecalho_despacho"),
    "informacao tecnica": (DOC_CLASS_INFORMACAO_TECNICA, "cabecalho_informacao_tecnica"),
    "nota tecnica": (DOC_CLASS_NOTA_TECNICA, "cabecalho_nota_tecnica"),
}

ACT_HEADER_MARKERS = (
    "acordo de cooperacao tecnica",
    "acordo de cooperacao",
)

FINAL_REPORT_MARKERS = (
    "relatorio final",
    "relatorio de encerramento",
    "relatorio conclusivo",
    "relatorio final de execucao",
)

REPORT_MARKERS = (
    "relatorio conjunto de execucao",
    "relatorio conjunto de atividades",
    "relatorio de execucao",
    "relatorio das atividades",
)

FINALIZATION_MARKERS = (
    "apos o encerramento",
    "apos o termino",
    "ao termino da vigencia",
    "ao final da vigencia",
    "por ocasiao do encerramento",
    "encerramento da parceria",
    "encerramento do ajuste",
)

PERIODIC_REPORT_MARKERS = (
    "mensal",
    "bimestral",
    "trimestral",
    "quadrimestral",
    "semestral",
    "anual",
)

INTERNAL_ACT_MARKERS = (
    "censipam",
    "ministerio da defesa",
    "centro gestor e operacional do sistema de protecao da amazonia",
    "sistema de protecao da amazonia",
)


def _log(logger: Any, level: str, msg: str, *args: Any) -> None:
    if logger is None:
        return
    try:
        fn = getattr(logger, level, None)
        if callable(fn):
            fn(msg, *args)
    except Exception:
        return


def _clean_spaces(value: str) -> str:
    return " ".join((value or "").replace("\r", "\n").split()).strip()


def _maybe_fix_mojibake(value: str) -> str:
    text = value or ""
    if not text or not any(marker in text for marker in ("Ã", "Â", "â", "\ufffd")):
        return text

    repaired = text
    for _ in range(2):
        candidate = repaired
        for source_encoding in ("latin1", "cp1252"):
            try:
                candidate = repaired.encode(source_encoding).decode("utf-8")
                break
            except UnicodeError:
                candidate = repaired
        if candidate == repaired:
            break
        repaired = candidate
        if not any(marker in repaired for marker in ("Ã", "Â", "â", "\ufffd")):
            break
    return repaired


def _prepare_text(value: str) -> str:
    text = _maybe_fix_mojibake(value or "")
    if not text:
        return ""
    replacements = {
        "\u00a0": " ",
        "\ufb01": "fi",
        "\ufb02": "fl",
        "\ufb00": "ff",
        "\ufb03": "ffi",
        "\ufb04": "ffl",
        "Гўв‚¬вЂњ": "-",
        "Гўв‚¬вЂќ": "-",
        "вЂ“": "-",
        "вЂ”": "-",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"\bPORINTERM[EÉ]DIO\b", "POR INTERMEDIO", text, flags=re.IGNORECASE)
    text = re.sub(r"\bPARAOPERA[CÇ][AÃ]O\b", "PARA OPERACAO", text, flags=re.IGNORECASE)
    text = re.sub(r"(?<=[a-z\u00e0-\u00ff])(?=[A-Z\u00c0-\u00dd])", " ", text)
    text = re.sub(r"(?<=[A-Za-z\u00c0-\u00ff])(?=\d{4}\b)", " ", text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _normalize_text(value: str) -> str:
    text = _clean_spaces(_prepare_text(value))
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower().replace("\u00ba", "o").replace("\u00b0", "o")
    return re.sub(r"\s+", " ", text).strip()


def _trim_noise(value: str) -> str:
    prepared = _prepare_text(value)
    if not prepared:
        return ""
    for marker in INVALID_TAIL_MARKERS:
        match = re.search(re.escape(marker), prepared, flags=re.IGNORECASE)
        if match:
            prepared = prepared[: match.start()]
            break
    return prepared.strip()


def _has_content(value: str, min_alpha: int = 8) -> bool:
    cleaned = _trim_noise(value)
    return len(re.findall(r"[A-Za-z\u00c0-\u00ff]", cleaned)) >= min_alpha


def _text_blobs(
    snapshot: Dict[str, Any],
    collection_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    title = _prepare_text(str(snapshot.get("title", "") or ""))
    text = _trim_noise(str(snapshot.get("text", "") or ""))
    selected = _prepare_text(str((collection_context or {}).get("chosen_documento", "") or ""))
    lead = text[:LEAD_SCAN_CHARS].strip()
    header = text[:HEADER_SCAN_CHARS].strip()
    opening = text[:OPENING_SCAN_CHARS].strip()
    return {
        "title": title,
        "text": text,
        "selected": selected,
        "lead": lead,
        "header": header,
        "opening": opening,
        "normalized_title": _normalize_text(title),
        "normalized_text": _normalize_text(text),
        "normalized_selected": _normalize_text(selected),
        "normalized_lead": _normalize_text(lead),
        "normalized_header": _normalize_text(header),
        "normalized_opening": _normalize_text(opening),
    }


def _classification_record(doc_class: str, reason: str) -> Dict[str, Any]:
    return {
        "doc_class": doc_class,
        "resolved_document_type": DOC_CLASS_RESOLVED_TYPE.get(doc_class, RESOLVED_TYPE_ACT_RELATED),
        "snapshot_prefix": DOC_CLASS_SNAPSHOT_PREFIX.get(doc_class, SNAPSHOT_PREFIX_ACT),
        "classification_reason": reason,
        "classification_priority": DOC_CLASS_PRIORITY.get(doc_class, 0),
    }


def _classify_snapshot_core(
    snapshot: Dict[str, Any],
    collection_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    blobs = _text_blobs(snapshot, collection_context)
    rejection_blob = " ".join(
        part for part in (blobs["normalized_title"], blobs["normalized_lead"], blobs["normalized_selected"]) if part
    )
    header_blob = " ".join(
        part for part in (blobs["normalized_title"], blobs["normalized_header"], blobs["normalized_selected"]) if part
    )
    opening_blob = " ".join(
        part for part in (blobs["normalized_title"], blobs["normalized_opening"], blobs["normalized_selected"]) if part
    )
    full_blob = " ".join(part for part in (header_blob, blobs["normalized_text"]) if part)

    if not opening_blob:
        return _classification_record(DOC_CLASS_EMAIL_OUTRO, "snapshot_vazio")

    if "pesquisar no processo" in opening_blob or "tipos de documentos disponiveis neste processo" in opening_blob:
        return _classification_record(DOC_CLASS_STUB, "pagina_de_pesquisa")

    if "clique aqui para visualizar o conteudo deste documento" in opening_blob:
        return _classification_record(DOC_CLASS_STUB, "stub_visualizacao")

    title_or_selected_blob = " ".join(
        part for part in (blobs["normalized_title"], blobs["normalized_selected"]) if part
    )
    if "e-mail" in title_or_selected_blob or "email" in title_or_selected_blob:
        return _classification_record(DOC_CLASS_EMAIL_OUTRO, "email_ou_mensagem")

    for marker, (doc_class, reason) in ADMINISTRATIVE_HEADER_MARKERS.items():
        if marker not in rejection_blob:
            continue
        if marker == "oficio" and re.search(r"\boficio\b", rejection_blob) is None:
            continue
        return _classification_record(doc_class, reason)

    if re.search(r"\bmemo\b", rejection_blob):
        return _classification_record(DOC_CLASS_MEMORANDO, "cabecalho_memo")

    email_hits = sum(1 for marker in EMAIL_MARKERS if marker in full_blob)
    if email_hits >= 3 or "e-mail" in rejection_blob or "email" in rejection_blob:
        return _classification_record(DOC_CLASS_EMAIL_OUTRO, "email_ou_mensagem")

    for marker, (doc_class, reason) in HEADER_REJECTION_MARKERS.items():
        if marker not in rejection_blob:
            continue
        if marker == "oficio" and re.search(r"\boficio\b", rejection_blob) is None:
            continue
        if doc_class == DOC_CLASS_TED and not (
            "termo de execucao descentralizada" in rejection_blob or re.search(r"\bted\b", rejection_blob)
        ):
            continue
        return _classification_record(doc_class, reason)

    if re.search(r"\bmemo\b", rejection_blob):
        return _classification_record(DOC_CLASS_MEMORANDO, "cabecalho_memo")

    if any(marker in opening_blob for marker in ADMINISTRATIVE_RELATED_MARKERS):
        return _classification_record(
            DOC_CLASS_DOCUMENTO_ADMINISTRATIVO_RELACIONADO,
            "conteudo_administrativo_relacionado",
        )

    has_act_marker = any(marker in header_blob for marker in ACT_HEADER_MARKERS)
    has_contractual_language = any(marker in opening_blob for marker in CONTRACTUAL_MARKERS) or any(
        marker in opening_blob
        for marker in (
            "objeto do presente acordo",
            "para os fins que especifica",
            "resolvem firmar",
            "doravante denominado",
            "doravante denominada",
        )
    )
    if has_act_marker and has_contractual_language:
        if "acordo de cooperacao tecnica" in header_blob:
            return _classification_record(DOC_CLASS_ACT_FINAL, "cabecalho_act_tecnica_contratual")
        return _classification_record(DOC_CLASS_ACT_FINAL, "cabecalho_act_generico_contratual")

    return _classification_record(DOC_CLASS_EMAIL_OUTRO, "conteudo_nao_classificado")


def _accepted_doc_classes_for_requested_type(requested_type: str) -> Tuple[str, ...]:
    return {
        "act": (DOC_CLASS_ACT_FINAL,),
        "memorando": ADMINISTRATIVE_DOC_CLASSES,
        "ted": (DOC_CLASS_TED,),
    }.get(requested_type, ())


def _has_internal_act_context(
    snapshot: Dict[str, Any],
    collection_context: Optional[Dict[str, Any]] = None,
) -> bool:
    blobs = _text_blobs(snapshot, collection_context)
    opening_blob = " ".join(
        part
        for part in (
            blobs["normalized_title"],
            blobs["normalized_selected"],
            blobs["normalized_opening"],
        )
        if part
    )
    return any(marker in opening_blob for marker in INTERNAL_ACT_MARKERS)


def _extract_document_processes(snapshot: Dict[str, Any]) -> List[str]:
    source = " ".join(
        part
        for part in (
            _prepare_text(str(snapshot.get("title", "") or "")),
            _trim_noise(str(snapshot.get("text", "") or ""))[:PROCESS_SCAN_CHARS],
        )
        if part
    )
    normalized = _normalize_text(source)
    if not normalized:
        return []

    processes: List[str] = []
    for match in re.findall(PROCESS_PATTERN, normalized):
        cleaned = _clean_spaces(match)
        if cleaned and cleaned not in processes:
            processes.append(cleaned)
    return processes


def _assess_process_alignment(
    snapshot: Dict[str, Any],
    *,
    processo: str,
    has_internal_context: Optional[bool] = None,
) -> Dict[str, Any]:
    payload_processo = _clean_spaces(processo)
    document_processos = _extract_document_processes(snapshot)
    if not payload_processo or not document_processos:
        return {
            "status": "unknown",
            "document_processo": "",
            "document_processos": document_processos,
        }
    if payload_processo in document_processos:
        return {
            "status": "aligned",
            "document_processo": payload_processo,
            "document_processos": document_processos,
        }
    internal_context = has_internal_context if has_internal_context is not None else _has_internal_act_context(snapshot)
    return {
        "status": "external_reference" if internal_context else "material_mismatch",
        "document_processo": document_processos[0],
        "document_processos": document_processos,
    }


def classify_cooperation_snapshot(
    snapshot: Dict[str, Any],
    requested_type: str,
    collection_context: Optional[Dict[str, Any]] = None,
    processo: str = "",
) -> Dict[str, Any]:
    requested = _clean_spaces(requested_type or "").lower()
    base = _classify_snapshot_core(snapshot, collection_context)
    accepted_doc_classes = _accepted_doc_classes_for_requested_type(requested)
    doc_class = str(base.get("doc_class", "") or "")
    classification_reason = str(base.get("classification_reason", "") or "")
    is_canonical = doc_class in accepted_doc_classes
    discard_reason = "" if is_canonical else doc_class
    has_internal_context = False
    process_alignment = {"status": "unknown", "document_processo": "", "document_processos": []}

    if requested == "act" and doc_class == DOC_CLASS_ACT_FINAL:
        has_internal_context = _has_internal_act_context(snapshot, collection_context)
        process_alignment = _assess_process_alignment(
            snapshot,
            processo=processo,
            has_internal_context=has_internal_context,
        )
        if not has_internal_context:
            is_canonical = False
            classification_reason = "act_sem_marcador_interno"
            discard_reason = "act_sem_marcador_interno"
        elif process_alignment["status"] == "material_mismatch":
            is_canonical = False
            classification_reason = "processo_divergente_documento"
            discard_reason = "processo_divergente_documento"

    validation_status = VALIDATION_STATUS_VALID if is_canonical else VALIDATION_STATUS_RELATED
    if doc_class in {DOC_CLASS_STUB, DOC_CLASS_EMAIL_OUTRO}:
        validation_status = VALIDATION_STATUS_REJECTED

    publication_status = PUBLICATION_STATUS_GOLD if is_canonical else PUBLICATION_STATUS_SILVER
    normalization_status = "classificado_canonico" if is_canonical else "descartado_semantico"
    if publication_status == PUBLICATION_STATUS_GOLD:
        normalization_status = "publicado_canonico"

    result = {
        **base,
        "requested_type": requested,
        "accepted_doc_classes": accepted_doc_classes,
        "is_canonical_candidate": is_canonical,
        "validation_status": validation_status,
        "publication_status": publication_status,
        "normalization_status": normalization_status,
        "discard_reason": "" if is_canonical else discard_reason,
        "classification_reason": classification_reason,
        "requested_snapshot_prefix": REQUESTED_TYPE_TO_PREFIX.get(requested, SNAPSHOT_PREFIX_ACT),
        "has_internal_context": has_internal_context,
        "process_alignment_status": process_alignment["status"],
        "document_processo": process_alignment["document_processo"],
        "document_processos": process_alignment["document_processos"],
    }
    if requested == "act":
        result["process_affinity"] = assess_act_process_affinity(
            snapshot,
            current_process=processo,
            collection=collection_context,
        )
    return result


def classify_act_snapshot(
    snapshot: Dict[str, Any],
    collection_context: Optional[Dict[str, Any]] = None,
    processo: str = "",
) -> Dict[str, Any]:
    return classify_cooperation_snapshot(
        snapshot=snapshot,
        requested_type="act",
        collection_context=collection_context,
        processo=processo,
    )


def _read_json(path: Path) -> Dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _sanitize_filename_part(value: str, fallback: str = "sem_id") -> str:
    cleaned = re.sub(r"[^\w.-]+", "_", (value or "").strip()).strip("_")
    return (cleaned or fallback)[:80]


def _act_alias_path(output_dir: Path, processo: str) -> Path:
    return output_dir / f"{SNAPSHOT_PREFIX_ACT}_{_sanitize_filename_part(processo)}.json"


def _collect_act_snapshot_paths(output_dir: Path) -> List[Path]:
    candidate_dir = output_dir / "candidates"
    candidate_paths = sorted(candidate_dir.glob(f"{SNAPSHOT_PREFIX_ACT}_*.json"))
    if candidate_paths:
        return candidate_paths
    return sorted(output_dir.glob(f"{SNAPSHOT_PREFIX_ACT}_*.json"))


def _publish_act_alias(
    output_dir: Path,
    source_path: Path,
    processo: str,
    party_extraction: Optional[Dict[str, Any]] = None,
    process_affinity: Optional[Dict[str, Any]] = None,
) -> Path:
    alias_path = _act_alias_path(output_dir, processo)
    if source_path.resolve() != alias_path.resolve():
        alias_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, alias_path)
    if party_extraction is not None or process_affinity is not None:
        payload = _read_json(alias_path)
        analysis = payload.get("analysis")
        if not isinstance(analysis, dict):
            analysis = {}
            payload["analysis"] = analysis
        if party_extraction is not None:
            analysis["party_extraction"] = party_extraction
        if process_affinity is not None:
            analysis["process_affinity"] = process_affinity
        alias_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return alias_path


def _normalize_date_token(token: str) -> str:
    raw = _clean_spaces(_prepare_text(token))
    normalized = _normalize_text(raw)
    if not normalized:
        return ""

    for pattern in (
        r"(\d{4})-(\d{1,2})-(\d{1,2})",
        r"(\d{1,2})/(\d{1,2})/(\d{4})",
        r"(\d{1,2})\.(\d{1,2})\.(\d{4})",
        r"(\d{1,2})-(\d{1,2})-(\d{4})",
    ):
        match = re.fullmatch(pattern, normalized)
        if match:
            try:
                if pattern.startswith(r"(\d{4})"):
                    return date(int(match.group(1)), int(match.group(2)), int(match.group(3))).isoformat()
                return date(int(match.group(3)), int(match.group(2)), int(match.group(1))).isoformat()
            except ValueError:
                return ""

    month_map = {
        "jan": 1,
        "janeiro": 1,
        "fev": 2,
        "fevereiro": 2,
        "mar": 3,
        "marco": 3,
        "abr": 4,
        "abril": 4,
        "mai": 5,
        "maio": 5,
        "jun": 6,
        "junho": 6,
        "jul": 7,
        "julho": 7,
        "ago": 8,
        "agosto": 8,
        "set": 9,
        "setembro": 9,
        "out": 10,
        "outubro": 10,
        "nov": 11,
        "novembro": 11,
        "dez": 12,
        "dezembro": 12,
    }
    textual = re.fullmatch(r"(\d{1,2})\s+de\s+([a-zc]+)\s+de\s+(\d{4})", normalized)
    if textual:
        month = month_map.get(textual.group(2), 0)
        if month:
            try:
                return date(int(textual.group(3)), month, int(textual.group(1))).isoformat()
            except ValueError:
                return ""
    return ""


def _month_last_day(year: int, month: int) -> int:
    if month == 12:
        return 31
    return (date(year, month + 1, 1) - date(year, month, 1)).days


def _add_duration(start_iso: str, raw_amount: str, raw_unit: str) -> str:
    if not start_iso:
        return ""
    amount = int(raw_amount)
    base = datetime.fromisoformat(start_iso)
    unit = _normalize_text(raw_unit)
    if "ano" in unit:
        try:
            target = base.replace(year=base.year + amount).date()
        except ValueError:
            target = base.replace(month=2, day=28, year=base.year + amount).date()
        return (target - timedelta(days=1)).isoformat()
    if "mes" in unit:
        month_index = base.month - 1 + amount
        year = base.year + month_index // 12
        month = month_index % 12 + 1
        day = min(base.day, _month_last_day(year, month))
        target = date(year, month, day)
        return (target - timedelta(days=1)).isoformat()
    return ""


def _dedupe(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    for value in values:
        cleaned = _clean_spaces(value)
        if cleaned and cleaned not in out:
            out.append(cleaned)
    return out


def _extract_signature_dates(text: str) -> List[str]:
    prepared = _prepare_text(text)
    if not prepared:
        return []
    matches: List[str] = []
    for pattern in (
        rf"documento assinado eletronicamente .*? em {DATE_PATTERN}",
        rf"assinad[oa].{{0,200}}?\bem\s+{DATE_PATTERN}",
        rf"(?:bras[ií]lia(?:,\s*df)?|cidade)\s*,?\s*em\s+{DATE_PATTERN}",
    ):
        for match in re.finditer(pattern, prepared, flags=re.IGNORECASE | re.DOTALL):
            token = match.group(1)
            iso = _normalize_date_token(token)
            if iso:
                matches.append(iso)
    return _dedupe(matches)


def _extract_data_assinatura(snapshot: Dict[str, Any]) -> str:
    signatures = _extract_signature_dates(str(snapshot.get("text", "") or ""))
    return max(signatures) if signatures else ""


def _date_before(left: str, right: str) -> bool:
    try:
        return bool(left and right and date.fromisoformat(left) < date.fromisoformat(right))
    except ValueError:
        return False


def _first_clause_index(text: str) -> int:
    match = re.search(r"\bCL[\u00c1A]USULA\b", text, flags=re.IGNORECASE)
    return match.start() if match else -1


def _publication_context(text: str, token_start: int) -> str:
    start = max(0, token_start - 260)
    end = min(len(text), token_start + 420)
    return text[start:end]


def _publication_rejection_reason(
    *,
    text: str,
    token_start: int,
    publication_date: str,
    data_assinatura: str,
) -> str:
    context = _publication_context(text, token_start)
    normalized_context = _normalize_text(context)
    first_clause = _first_clause_index(text)
    is_preamble = first_clause >= 0 and token_start < first_clause
    has_publication_marker = any(
        marker in normalized_context
        for marker in ("publicad", "publicacao", "dou", "diario oficial")
    )
    has_nomination_marker = any(marker in normalized_context for marker in NOMINATION_PUBLICATION_MARKERS)
    if is_preamble and has_publication_marker and has_nomination_marker:
        return "data_publicacao_descartada_preambulo_nomeacao"
    if _date_before(publication_date, data_assinatura):
        return "data_publicacao_descartada_anterior_assinatura"
    if not any(marker in normalized_context for marker in PUBLICATION_CONTEXT_MARKERS):
        return "data_publicacao_descartada_contexto_incompativel"
    return ""


def _extract_data_publicacao_info(snapshot: Dict[str, Any], data_assinatura: str = "") -> Dict[str, str]:
    text = _prepare_text(str(snapshot.get("text", "") or ""))
    if not text:
        return {"data_publicacao": "", "warning": ""}
    patterns = (
        rf"(?:publicad[oa]|publicacao|publica[cç][aã]o)[^\n. ]*(?:[^\n.]{{0,180}}?){DATE_PATTERN}",
        rf"(?:diario oficial da uniao|dou)[^\n. ]*(?:[^\n.]{{0,180}}?){DATE_PATTERN}",
        rf"{DATE_PATTERN}[^\n.]{{0,120}}?(?:diario oficial da uniao|dou)",
    )
    candidates: List[Tuple[str, int, str]] = []
    rejected_reasons: List[str] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            token = match.group(1)
            iso = _normalize_date_token(token)
            if iso:
                candidates.append((iso, match.start(1), token))
    for publication_date, token_start, _ in sorted(candidates, key=lambda item: item[0]):
        rejection = _publication_rejection_reason(
            text=text,
            token_start=token_start,
            publication_date=publication_date,
            data_assinatura=data_assinatura,
        )
        if rejection:
            rejected_reasons.append(rejection)
            continue
        return {"data_publicacao": publication_date, "warning": ""}
    return {"data_publicacao": "", "warning": "; ".join(_dedupe(rejected_reasons))}


def _extract_data_publicacao(snapshot: Dict[str, Any], data_assinatura: str = "") -> str:
    return _extract_data_publicacao_info(snapshot, data_assinatura=data_assinatura)["data_publicacao"]


def _extract_preamble(text: str) -> str:
    prepared = _prepare_text(text)
    if not prepared:
        return ""
    stop = re.search(r"\bCL[\u00c1A]USULA\b", prepared, flags=re.IGNORECASE)
    if stop:
        return prepared[: stop.start()].strip()
    return prepared[:OPENING_SCAN_CHARS].strip()


def _extract_focus_window(text: str, patterns: Tuple[str, ...], window_chars: int = SECTION_WINDOW_CHARS) -> str:
    prepared = _prepare_text(text)
    if not prepared:
        return ""
    for pattern in patterns:
        match = re.search(pattern, prepared, flags=re.IGNORECASE)
        if match:
            return prepared[match.start() : match.start() + window_chars].strip()
    return ""


def _extract_section(text: str, heading_patterns: Tuple[str, ...]) -> str:
    prepared = _prepare_text(text)
    if not prepared:
        return ""
    for heading_pattern in heading_patterns:
        match = re.search(heading_pattern, prepared, flags=re.IGNORECASE)
        if not match:
            continue
        tail = prepared[match.end() :]
        stop = re.search(SECTION_STOP_PATTERN, tail, flags=re.IGNORECASE)
        return tail[: stop.start()].strip() if stop else tail.strip()
    return ""


def _clean_clause_value(value: str) -> str:
    cleaned = _trim_noise(value)
    cleaned = re.split(r"\bSubcl[a\u00e1]usula\b", cleaned, maxsplit=1, flags=re.IGNORECASE)[0]
    cleaned = re.sub(r"^[\s\-:.;]+", "", cleaned)
    return _clean_spaces(cleaned)


ACT_NUMBER_VALUE_PATTERN = (
    r"(?:\d{1,4}\s*/\s*\d{2,4}"
    r"|\d{1,4}-\d{4}(?:/[a-z0-9.-]+)+)"
)
ACT_NUMBER_MARKER_PATTERN = r"(?:\bact\b|\bacordo de cooperacao tecnica\b|\bacordo de cooperacao\b)"
ACT_NUMBER_LABEL_PATTERN = r"(?:n\s*(?:o|\.|umero)|numero)"


def _numero_comparison_key(value: str) -> str:
    normalized = _normalize_text(value).replace(" ", "")
    match = re.fullmatch(r"0*(\d+)/(0*\d+)", normalized)
    if match:
        return f"{int(match.group(1))}/{int(match.group(2))}"
    return normalized


def _numero_evidence(
    *, value: str, source_type: str, evidence: str, rule_id: str, confidence: str, priority: int
) -> Dict[str, Any]:
    return {
        "value": _clean_spaces(value).rstrip(".,;:"),
        "source_type": source_type,
        "evidence": _clean_spaces(evidence)[:300],
        "rule_id": rule_id,
        "confidence": confidence,
        "priority": priority,
    }


def _collect_numero_from_source(
    source: str,
    *,
    source_type: str,
    confidence: str,
    priority: int,
    rule_prefix: str,
    contextual: bool = False,
) -> List[Dict[str, Any]]:
    evidences: List[Dict[str, Any]] = []
    prepared = _prepare_text(source)
    if not prepared:
        return evidences
    chunks = prepared.splitlines() if contextual else [prepared]
    direct_pattern = rf"{ACT_NUMBER_MARKER_PATTERN}\s*(?:{ACT_NUMBER_LABEL_PATTERN})?\s*[:.\-]?\s*({ACT_NUMBER_VALUE_PATTERN})"
    contextual_pattern = rf"{ACT_NUMBER_MARKER_PATTERN}.{{0,160}}?{ACT_NUMBER_LABEL_PATTERN}\s*[:.\-]?\s*({ACT_NUMBER_VALUE_PATTERN})"
    placeholder_pattern = rf"{ACT_NUMBER_MARKER_PATTERN}\s*(?:{ACT_NUMBER_LABEL_PATTERN})?\s*[:.\-]?\s*((?:x+[/.-]?x+|x+/20xx|s\s*/\s*n(?:umero|o)?))"
    for chunk in chunks:
        normalized = _normalize_text(chunk)
        if not normalized:
            continue
        patterns = [(f"{rule_prefix}.adjacent", direct_pattern)]
        if contextual:
            patterns.append((f"{rule_prefix}.contextual", contextual_pattern))
        patterns.append((f"{rule_prefix}.placeholder", placeholder_pattern))
        for rule_id, pattern in patterns:
            for match in re.finditer(pattern, normalized, flags=re.IGNORECASE):
                evidence = _numero_evidence(
                    value=match.group(1),
                    source_type=source_type,
                    evidence=chunk,
                    rule_id=rule_id,
                    confidence=confidence,
                    priority=priority,
                )
                if not any(
                    item["rule_id"] == evidence["rule_id"]
                    and _numero_comparison_key(item["value"]) == _numero_comparison_key(evidence["value"])
                    for item in evidences
                ):
                    evidences.append(evidence)
    return evidences


def _extract_numero_acordo(snapshot: Dict[str, Any], collection: Dict[str, Any]) -> Dict[str, Any]:
    evidences: List[Dict[str, Any]] = []
    evidences.extend(
        _collect_numero_from_source(
            _trim_noise(str(snapshot.get("text", "") or ""))[:HEADER_SCAN_CHARS],
            source_type=SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_HIGH,
            priority=300,
            rule_prefix="act.numero.header",
            contextual=True,
        )
    )
    evidences.extend(
        _collect_numero_from_source(
            str(collection.get("chosen_documento", "") or ""),
            source_type=SOURCE_DOCUMENT_TITLE,
            confidence=CONFIDENCE_MEDIUM,
            priority=200,
            rule_prefix="act.numero.selected_title",
        )
    )
    evidences.extend(
        _collect_numero_from_source(
            str(snapshot.get("title", "") or ""),
            source_type=SOURCE_DOCUMENT_METADATA,
            confidence=CONFIDENCE_MEDIUM,
            priority=100,
            rule_prefix="act.numero.snapshot_title",
        )
    )
    valid = [item for item in evidences if not _is_placeholder_numero_acordo(str(item.get("value", "")))]
    if not valid:
        return {"value": "", "field_source": "", "source_type": SOURCE_MISSING, "confidence": CONFIDENCE_LOW, "evidence": "", "warning": "numero_placeholder" if evidences else "", "evidences": evidences}
    chosen = max(valid, key=lambda item: int(item["priority"]))
    chosen_key = _numero_comparison_key(chosen["value"])
    conflicts = [item for item in valid if _numero_comparison_key(item["value"]) != chosen_key]
    warning = ""
    if conflicts:
        details = ", ".join(f"{item['source_type']}={item['value']}" for item in conflicts)
        warning = f"numero_acordo_conflict: selected={chosen['source_type']}={chosen['value']}; alternatives={details}"
    return {
        "value": chosen["value"],
        "field_source": chosen["rule_id"],
        "source_type": chosen["source_type"],
        "confidence": chosen["confidence"],
        "evidence": chosen["evidence"],
        "warning": warning,
        "evidences": evidences,
    }


def _is_placeholder_numero_acordo(value: str) -> bool:
    normalized = _normalize_text(value).replace(" ", "")
    if not normalized:
        return False
    if re.fullmatch(r"x+[/.-]?(?:x+|\d*x+\d*)", normalized):
        return True
    if re.fullmatch(r"s[/.-]?n(?:o|umero)?", normalized):
        return True
    return any(marker in normalized for marker in ("semnumero", "semnumeracao", "xxxxx", "xx/20xx"))


def _extract_document_process(snapshot: Dict[str, Any]) -> str:
    processes = _extract_document_processes(snapshot)
    return processes[0] if processes else ""


def _extract_explicit_period(prepared: str) -> Tuple[str, str]:
    match = re.search(
        rf"(?:de|entre)\s+{DATE_PATTERN}\s+(?:a|ate|at[e\u00e9]|-)\s+{DATE_PATTERN}",
        prepared,
        flags=re.IGNORECASE,
    )
    if not match:
        return ("", "")
    start_iso = _normalize_date_token(match.group(1))
    end_iso = _normalize_date_token(match.group(2))
    return (start_iso, end_iso) if start_iso and end_iso else ("", "")


def _extract_first_date_after_marker(prepared: str, marker: str) -> str:
    match = re.search(rf"{re.escape(marker)}.{{0,160}}?{DATE_PATTERN}", prepared, flags=re.IGNORECASE | re.DOTALL)
    return _normalize_date_token(match.group(1)) if match else ""


def _extract_vigencia_raw(snapshot: Dict[str, Any]) -> str:
    text = str(snapshot.get("text", "") or "")
    section = _extract_section(
        text,
        (
            r"(?:\d+\s*[.)-]\s*)?CL[\u00c1A]USULA\s+(?:NONA|S[\u00c9E]TIMA|OITAVA|QUINTA|D[\u00c9E]CIMA(?:\s+\w+)*)\s*[-â€“â€”]?\s*(?:DO\s+)?PRAZO(?:\s+E\s+VIG[\u00caE]NCIA)?",
            r"(?:\d+\s*[.)-]\s*)?CL[\u00c1A]USULA\s+.*?\s*[-â€“â€”]?\s*(?:DA|DO)\s+VIG[\u00caE]NCIA",
            r"\bPRAZO\s+E\s+VIG[\u00caE]NCIA\b",
            r"\bPRAZO\s+DE\s+VIG[\u00caE]NCIA\b",
            r"\bVIG[\u00caE]NCIA\b\s*:",
        ),
    )
    if not section:
        section = _extract_focus_window(
            text,
            (
                r"\bPRAZO\s+DE\s+VIG[\u00caE]NCIA\b",
                r"\bPRAZO\s+E\s+VIG[\u00caE]NCIA\b",
                r"\bVIG[\u00caE]NCIA\b",
            ),
        )
    return _clean_clause_value(section) if section else ""


def resolve_act_vigencia(
    vigencia_raw: str,
    data_assinatura: str = "",
    data_publicacao: str = "",
    outras_datas: Optional[Iterable[str]] = None,
) -> Dict[str, str]:
    prepared = _prepare_text(vigencia_raw or "")
    normalized = _normalize_text(prepared)
    result = {
        "vigencia_inicio": "",
        "vigencia_fim": "",
        "field_source_vigencia": "",
        "warning": "",
        "amount": "",
        "unit": "",
        "anchor": "",
    }
    if not normalized:
        return result

    explicit_start, explicit_end = _extract_explicit_period(prepared)
    if explicit_start and explicit_end:
        return {
            **result,
            "vigencia_inicio": explicit_start,
            "vigencia_fim": explicit_end,
            "field_source_vigencia": "clausula_vigencia_periodo_explicito",
            "anchor": "data_explicita",
        }

    duration = re.search(r"(\d{1,3})\s*(?:\([^)]+\))?\s+(mes(?:es)?|anos?)", normalized, flags=re.IGNORECASE)
    if duration:
        result["amount"] = duration.group(1)
        result["unit"] = duration.group(2)

    start_match = re.search(
        rf"(?:a partir de|a contar de|contados? de|contado da)\s+{DATE_PATTERN}",
        prepared,
        flags=re.IGNORECASE,
    )
    if start_match:
        start_iso = _normalize_date_token(start_match.group(1))
        if start_iso:
            return {
                **result,
                "vigencia_inicio": start_iso,
                "vigencia_fim": _add_duration(start_iso, result["amount"], result["unit"]) if duration else "",
                "field_source_vigencia": "clausula_vigencia_data_inicial_explicita",
                "anchor": "data_explicita",
            }

    if "assinatura" in normalized:
        result["anchor"] = "assinatura"
        source = "clausula_vigencia_ultima_assinatura" if "ultima assinatura" in normalized else "clausula_vigencia_assinatura"
        if not data_assinatura:
            return {**result, "warning": "vigencia_dependente_assinatura_sem_data"}
        return {
            **result,
            "vigencia_inicio": data_assinatura,
            "vigencia_fim": _add_duration(data_assinatura, result["amount"], result["unit"]) if duration else "",
            "field_source_vigencia": source,
        }

    if "publicacao" in normalized or "diario oficial" in normalized or re.search(r"\bdou\b", normalized):
        result["anchor"] = "publicacao"
        if not data_publicacao:
            data_publicacao = _extract_first_date_after_marker(prepared, "publicacao")
        if not data_publicacao:
            return {**result, "warning": "vigencia_dependente_publicacao_sem_data"}
        if _date_before(data_publicacao, data_assinatura):
            return {
                **result,
                "warning": "vigencia_dependente_publicacao_sem_data; data_publicacao_descartada_anterior_assinatura",
            }
        return {
            **result,
            "vigencia_inicio": data_publicacao,
            "vigencia_fim": _add_duration(data_publicacao, result["amount"], result["unit"]) if duration else "",
            "field_source_vigencia": "clausula_vigencia_publicacao",
        }

    normalized_other_dates = [_normalize_date_token(value) for value in (outras_datas or [])]
    normalized_other_dates = [value for value in normalized_other_dates if value]
    if duration and len(normalized_other_dates) == 1:
        start_iso = normalized_other_dates[0]
        return {
            **result,
            "vigencia_inicio": start_iso,
            "vigencia_fim": _add_duration(start_iso, result["amount"], result["unit"]),
            "field_source_vigencia": "clausula_vigencia_data_contextual",
            "anchor": "data_contextual",
        }

    return result


def _extract_vigencia_legacy(snapshot: Dict[str, Any]) -> Tuple[str, str, str, str]:
    text = str(snapshot.get("text", "") or "")
    section = _extract_section(
        text,
        (
            r"(?:\d+\s*[.)-]\s*)?CL[\u00c1A]USULA\s+(?:NONA|S[\u00c9E]TIMA|OITAVA|QUINTA|D[\u00c9E]CIMA(?:\s+\w+)*)\s*[-–—]?\s*(?:DO\s+)?PRAZO(?:\s+E\s+VIG[\u00caE]NCIA)?",
            r"(?:\d+\s*[.)-]\s*)?CL[\u00c1A]USULA\s+.*?\s*[-–—]?\s*(?:DA|DO)\s+VIG[\u00caE]NCIA",
            r"\bPRAZO\s+E\s+VIG[\u00caE]NCIA\b",
            r"\bPRAZO\s+DE\s+VIG[\u00caE]NCIA\b",
            r"\bVIG[\u00caE]NCIA\b\s*:",
        ),
    )
    if not section:
        section = _extract_focus_window(
            text,
            (
                r"\bPRAZO\s+DE\s+VIG[\u00caE]NCIA\b",
                r"\bPRAZO\s+E\s+VIG[\u00caE]NCIA\b",
                r"\bVIG[\u00caE]NCIA\b",
            ),
        )
        if not section:
            return ("", "", "", "")

    prepared = _prepare_text(section)
    explicit_start, explicit_end = _extract_explicit_period(prepared)
    if explicit_start and explicit_end:
        return (explicit_start, explicit_end, "clausula_vigencia_periodo_explicito", "")

    normalized = _normalize_text(prepared)
    duration = re.search(r"(\d{1,3})\s*(?:\([^)]+\))?\s+(mes(?:es)?|anos?)", normalized, flags=re.IGNORECASE)

    start_match = re.search(
        rf"(?:a partir de|a contar de|contados? de|contado da)\s+{DATE_PATTERN}",
        prepared,
        flags=re.IGNORECASE,
    )
    if start_match:
        start_iso = _normalize_date_token(start_match.group(1))
        if not start_iso:
            return ("", "", "", "")
        if duration:
            end_iso = _add_duration(start_iso, duration.group(1), duration.group(2))
            return (start_iso, end_iso, "clausula_vigencia_data_inicial_explicita", "")
        return (start_iso, "", "clausula_vigencia_data_inicial_explicita", "")

    if "assinatura" in normalized:
        signatures = _extract_signature_dates(text)
        start_iso = max(signatures) if signatures else ""
        if not start_iso:
            return ("", "", "", "vigencia_dependente_assinatura_sem_data")
        if duration:
            end_iso = _add_duration(start_iso, duration.group(1), duration.group(2))
            source = "clausula_vigencia_ultima_assinatura" if "ultima assinatura" in normalized else "clausula_vigencia_assinatura"
            return (start_iso, end_iso, source, "")
        source = "clausula_vigencia_ultima_assinatura" if "ultima assinatura" in normalized else "clausula_vigencia_assinatura"
        return (start_iso, "", source, "")

    if "publicacao" in normalized:
        publication_date = _extract_first_date_after_marker(prepared, "publicacao")
        if not publication_date:
            return ("", "", "", "vigencia_dependente_publicacao_sem_data")
        signatures = _extract_signature_dates(text)
        signature_date = max(signatures) if signatures else ""
        if _date_before(publication_date, signature_date):
            return (
                "",
                "",
                "",
                "vigencia_dependente_publicacao_sem_data; data_publicacao_descartada_anterior_assinatura",
            )
        if duration:
            end_iso = _add_duration(publication_date, duration.group(1), duration.group(2))
            return (publication_date, end_iso, "clausula_vigencia_publicacao_explicita", "")
        return (publication_date, "", "clausula_vigencia_publicacao_explicita", "")

    return ("", "", "", "")


def _extract_vigencia(snapshot: Dict[str, Any]) -> Tuple[str, str, str, str]:
    raw = _extract_vigencia_raw(snapshot)
    data_assinatura = _extract_data_assinatura(snapshot)
    resolved = resolve_act_vigencia(
        raw,
        data_assinatura=data_assinatura,
        data_publicacao=_extract_data_publicacao(snapshot, data_assinatura=data_assinatura),
        outras_datas=_extract_signature_dates(str(snapshot.get("text", "") or "")),
    )
    return (
        resolved["vigencia_inicio"],
        resolved["vigencia_fim"],
        resolved["field_source_vigencia"],
        resolved["warning"],
    )


def _extract_vigencia_rule(snapshot: Dict[str, Any]) -> Dict[str, str]:
    text = str(snapshot.get("text", "") or "")
    section = _extract_section(
        text,
        (
            r"(?:\d+\s*[.)-]\s*)?CL[\u00c1A]USULA\s+(?:NONA|S[\u00c9E]TIMA|OITAVA|QUINTA|D[\u00c9E]CIMA(?:\s+\w+)*)\s*[-–—]?\s*(?:DO\s+)?PRAZO(?:\s+E\s+VIG[\u00caE]NCIA)?",
            r"(?:\d+\s*[.)-]\s*)?CL[\u00c1A]USULA\s+.*?\s*[-–—]?\s*(?:DA|DO)\s+VIG[\u00caE]NCIA",
            r"\bPRAZO\s+E\s+VIG[\u00caE]NCIA\b",
            r"\bPRAZO\s+DE\s+VIG[\u00caE]NCIA\b",
            r"\bVIG[\u00caE]NCIA\b\s*:",
        ),
    )
    if not section:
        section = _extract_focus_window(
            text,
            (
                r"\bPRAZO\s+DE\s+VIG[\u00caE]NCIA\b",
                r"\bPRAZO\s+E\s+VIG[\u00caE]NCIA\b",
                r"\bVIG[\u00caE]NCIA\b",
            ),
        )
    normalized = _normalize_text(section)
    if not normalized:
        return {"amount": "", "unit": "", "anchor": ""}

    duration = re.search(r"(\d{1,3})\s*(?:\([^)]+\))?\s+(mes(?:es)?|anos?)", normalized, flags=re.IGNORECASE)
    anchor = ""
    if "publicacao" in normalized:
        anchor = "publicacao"
    elif "assinatura" in normalized:
        anchor = "assinatura"
    elif re.search(r"\ba partir de\b|\ba contar de\b", normalized):
        anchor = "data_explicita"
    return {
        "amount": duration.group(1) if duration else "",
        "unit": duration.group(2) if duration else "",
        "anchor": anchor,
    }


def _looks_like_internal_orgao(value: str) -> bool:
    normalized = _normalize_text(value)
    return any(
        marker in normalized
        for marker in (
            "ministerio da defesa",
            "centro gestor e operacional do sistema de protecao da amazonia",
            "censipam",
            "uniao",
        )
    )


def _clean_party_candidate(value: str) -> str:
    candidate = _clean_spaces(value)
    candidate = re.sub(r"^(?:e\s+)?(?:a|o|as|os)\s+", "", candidate, flags=re.IGNORECASE)
    for pattern in (
        r",?\s+por\s+(?:interm[eé]dio|meio)\s+d[aoe]\s+.*$",
        r",\s+neste\s+ato.*$",
        r",\s+doravante.*$",
        r",\s+com\s+sede.*$",
        r",\s*inscrit[oa].*$",
        r",\s+qualificad[oa].*$",
        r",\s+portador(?:a)?\s+.*$",
    ):
        candidate = re.sub(pattern, "", candidate, flags=re.IGNORECASE)
    candidate = re.sub(r"\s*\([^)]*\)", "", candidate)
    return candidate.strip(" ,.;:-")


def _split_orgao_candidate(value: str) -> Dict[str, str]:
    prepared = _clean_spaces(_prepare_text(value))
    intermediary = ""
    split = re.search(
        r"\bpor\s+(?:interm[eé]dio|meio)\s+d[aoe]\s+(.+)$",
        prepared,
        flags=re.IGNORECASE,
    )
    if split:
        intermediary = _clean_party_candidate(split.group(1))
    cleaned = _clean_party_candidate(prepared)
    name = cleaned
    sigla = ""
    paren_sigla = re.search(r"\(([A-Z0-9]{2,}(?:/[A-Z0-9]{2,})?)\)\s*$", prepared)
    if paren_sigla:
        sigla = paren_sigla.group(1)
        name = _clean_party_candidate(prepared[: paren_sigla.start()])
        cleaned = f"{name} - {sigla}" if name else sigla
    sigla_match = re.search(r"\s[-–—]\s*([A-Z0-9]{2,}(?:/[A-Z0-9]{2,})?)\s*$", cleaned)
    if sigla_match and not sigla:
        sigla = sigla_match.group(1)
        name = cleaned[: sigla_match.start()].strip(" -–—")
    return {
        "orgao_convenente": cleaned,
        "orgao_convenente_nome": name,
        "orgao_convenente_sigla": sigla,
        "orgao_intermediario": intermediary,
    }


def _extract_orgao_intermediario(snapshot: Dict[str, Any]) -> str:
    preamble = _extract_preamble(str(snapshot.get("text", "") or ""))
    if not preamble:
        return ""
    match = re.search(
        r"\bpor\s+(?:interm[eé]dio|meio)\s+d[aoe]\s+(.+?)(?:,\s+para os fins que especifica|,\s+doravante|,\s+neste ato|\.|\n|$)",
        _prepare_text(preamble),
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return ""
    candidate = _clean_party_candidate(match.group(1))
    return "" if _looks_like_internal_orgao(candidate) else candidate


def _extract_orgao_convenente_legacy(snapshot: Dict[str, Any]) -> Tuple[str, str]:
    preamble = _extract_preamble(str(snapshot.get("text", "") or ""))
    if not preamble:
        return ("", "")

    for pattern in (
        r"que entre si celebram\s+a\s+uniao,\s+por\s+interm[eé]dio\s+do\s+(.+?)(?:,\s*|\s+)e\s+o\s+ministerio\s+da\s+defesa",
        r"que entre si celebram.*?censipam\s+e\s+(?:a|o|as|os)\s+(.+?)(?:,\s+para os fins que especifica|,\s+doravante|,\s+neste ato)",
        r"que entre si celebram\s+(?:a|o|as|os)\s+(.+?)(?:,\s*|\s+)e\s+(?:o|a)\s+(?:centro gestor e operacional do sistema de protecao da amazonia|censipam)",
    ):
        match = re.search(pattern, preamble, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        candidate = _clean_party_candidate(match.group(1))
        if _has_content(candidate, min_alpha=4) and not _looks_like_internal_orgao(candidate):
            return (candidate, "preambulo_qualificacao_partes")

    for paragraph in re.split(r"\n\s*\n", _prepare_text(preamble)):
        snippet = _clean_spaces(paragraph[:900])
        if not snippet:
            continue
        for pattern in (
            r"^(?:a|o)\s+(.+?)(?:,\s+com\s+sede|,\s*inscrit[oa]|,\s+neste\s+ato|,\s+doravante)",
            r"^(?:a|o)\s+(.+?)(?:\s+com\s+sede|\s*inscrit[oa])",
        ):
            match = re.search(pattern, snippet, flags=re.IGNORECASE)
            if not match:
                continue
            candidate = _clean_party_candidate(match.group(1))
            if _has_content(candidate, min_alpha=4) and not _looks_like_internal_orgao(candidate):
                return (candidate, "preambulo_paragrafo_partes")

    normalized_preamble = _normalize_text(preamble)
    for pattern in (
        r"que entre si celebram\s+a\s+uniao,\s+por\s+intermedio\s+do\s+(.+?)(?:,\s*|\s+)e\s+o\s+ministerio\s+da\s+defesa",
        r"que entre si celebram.*?censipam\s+e\s+(?:a|o|as|os)\s+(.+?)(?:,\s+para os fins que especifica|,\s+doravante|,\s+neste ato)",
        r"que entre si celebram\s+(?:a|o|as|os)\s+(.+?)(?:,\s*|\s+)e\s+(?:o|a)\s+(?:centro gestor e operacional do sistema de protecao da amazonia|censipam)",
    ):
        match = re.search(pattern, normalized_preamble, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            continue
        candidate = _clean_party_candidate(match.group(1))
        if _has_content(candidate, min_alpha=4) and not _looks_like_internal_orgao(candidate):
            return (candidate, "preambulo_normalizado_partes")

    for source in (preamble, normalized_preamble):
        match = re.search(
            r"\bcensipam\s+e\s+(?:a|o|as|os)\s+(.+?)(?:\s+para os fins que especifica|\.|\n|$)",
            source,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if match:
            candidate = _clean_party_candidate(match.group(1))
            if _has_content(candidate, min_alpha=4) and not _looks_like_internal_orgao(candidate):
                return (candidate, "preambulo_partes_fallback")

    return ("", "")


_PARTY_CONFIDENCE_SCORE = {
    CONFIDENCE_HIGH: 3,
    CONFIDENCE_MEDIUM: 2,
    CONFIDENCE_LOW: 1,
}

_NON_PARTY_ROLE_PATTERNS = (
    ("interveniente", r"\binterveniente\b"),
    ("anuente", r"\banuente\b"),
    ("beneficiario", r"\bbenefici[aá]ri[oa]\b"),
    ("executor", r"\bexecutor(?:a)?\b"),
)


def _party_key(value: str) -> str:
    normalized = _normalize_text(_clean_party_candidate(value))
    normalized = re.sub(r"\bpara\s+os\s+fins.*$", "", normalized)
    normalized = re.sub(r"\b(?:a|o|as|os)\b", " ", normalized)
    return re.sub(r"[^a-z0-9]+", " ", normalized).strip()


def _party_sigla(value: str) -> str:
    prepared = _clean_spaces(_prepare_text(value))
    match = re.search(
        r"(?:\(([A-Z0-9]{2,}(?:/[A-Z0-9]{2,})?)\)|[-–—]\s*([A-Z0-9]{2,}(?:/[A-Z0-9]{2,})?)|doravante\s+denominad[oa]\s+([A-Z0-9]{2,}))\s*$",
        prepared,
        flags=re.IGNORECASE,
    )
    return next((group.upper() for group in match.groups() if group), "") if match else ""


def _candidate_role(evidence: str, default: str = "participe_direto") -> str:
    normalized = _normalize_text(evidence)
    for role, pattern in _NON_PARTY_ROLE_PATTERNS:
        if re.search(pattern, normalized, flags=re.IGNORECASE):
            return role
    return default


def _make_party_candidate(
    value: str,
    *,
    role: str,
    zone: str,
    evidence: str,
    position: int,
    confidence: str,
    source_rule: str,
) -> Optional[Dict[str, Any]]:
    raw_value = _clean_spaces(value)
    parts = _split_orgao_candidate(raw_value)
    cleaned = parts.get("orgao_convenente", "")
    if not _has_content(cleaned, min_alpha=4):
        return None
    resolved_role = "parte_interna" if _looks_like_internal_orgao(cleaned) else role
    return {
        "name": parts.get("orgao_convenente_nome", "") or cleaned,
        "sigla": parts.get("orgao_convenente_sigla", "") or _party_sigla(raw_value),
        "normalized_value": cleaned,
        "role": resolved_role,
        "zone": zone,
        "evidence": _clean_spaces(evidence)[:700],
        "position": max(position, 0),
        "confidence": confidence,
        "institutional_link": bool(parts.get("orgao_convenente_nome") and (parts.get("orgao_convenente_sigla") or _party_sigla(raw_value))),
        "source_rule": source_rule,
        "rejection_reason": "parte_interna" if resolved_role == "parte_interna" else "",
    }


def _extract_party_candidates(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    text = str(snapshot.get("text", "") or "")
    prepared = _prepare_text(text)
    preamble = _extract_preamble(text)
    candidates: List[Dict[str, Any]] = []

    def add(value: str, role: str, zone: str, evidence: str, confidence: str, rule: str) -> None:
        candidate = _make_party_candidate(
            value,
            role=role,
            zone=zone,
            evidence=evidence,
            position=prepared.lower().find(_prepare_text(value).lower()),
            confidence=confidence,
            source_rule=rule,
        )
        if candidate:
            candidates.append(candidate)

    # Preserve the proven patterns as a compatibility candidate while richer
    # qualification evidence is collected below.
    legacy_value, legacy_source = _extract_orgao_convenente_legacy(snapshot)
    if legacy_value:
        confidence = CONFIDENCE_HIGH if legacy_source == "preambulo_paragrafo_partes" else CONFIDENCE_MEDIUM
        add(legacy_value, "participe_direto", "formula_celebratoria", legacy_value, confidence, legacy_source)

    opening = _prepare_text(preamble or prepared[:OPENING_SCAN_CHARS])
    opening_normalized = _normalize_text(opening)
    if "censipam" in opening_normalized or "centro gestor e operacional do sistema de protecao da amazonia" in opening_normalized:
        add(
            "Centro Gestor e Operacional do Sistema de Protecao da Amazonia - CENSIPAM",
            "parte_interna",
            "qualificacao_inicial",
            "CENSIPAM",
            CONFIDENCE_HIGH,
            "act.partes.interna_censipam",
        )

    for table in snapshot.get("tables", []) or []:
        rows = table.get("rows", []) if isinstance(table, dict) else []
        for row in rows:
            cells = [_clean_spaces(str(cell or "")) for cell in row] if isinstance(row, list) else []
            for index, cell in enumerate(cells):
                label = _normalize_text(cell)
                if len(label) > 60 or not re.fullmatch(
                    r"(?:(?:identificacao|qualificacao)\s+d[oa]s?\s+)?(?:participe|parte)\s*(?:1|2|3|i{1,3})?\s*:??",
                    label,
                ):
                    continue
                value = cells[index + 1] if index + 1 < len(cells) else re.sub(r"^.*?[:\-–—]\s*", "", cell)
                add(value, _candidate_role(cell), "tabela_identificacao", " | ".join(cells), CONFIDENCE_HIGH, "act.partes.tabela_identificacao")

    # Formula celebratoria: tolerate an omitted article and either ordering of
    # CENSIPAM and the counterparty. The delimiter prevents body text capture.
    after_internal_patterns = (
        (opening, r"\bcensipam\s*,?\s+e\s+(?:(?:a|o|as|os)\s+)?(.+?)(?=(?:,\s*|\s+)(?:para\s+os\s+fins|doravante|neste\s+ato|por\s+(?:meio|interm[eé]dio))|\.\s|$)"),
        (opening_normalized, r"\bcentro\s+gestor\s+e\s+operacional\s+do\s+sistema\s+de\s+protecao\s+da\s+amazonia\s*[-–—]?\s*censipam\s*,?\s+e\s+(?:(?:a|o|as|os)\s+)?(.+?)(?=(?:,\s*|\s+)(?:para\s+os\s+fins|doravante|neste\s+ato|por\s+(?:meio|intermedio))|\.\s|$)"),
    )
    for source, pattern in after_internal_patterns:
        match = re.search(pattern, source, flags=re.IGNORECASE | re.DOTALL)
        if match:
            add(match.group(1), "participe_direto", "formula_celebratoria", match.group(0), CONFIDENCE_MEDIUM, "act.partes.formula_celebratoria")

    before_internal = re.search(
        r"que\s+entre\s+si\s+celebram\s+(?:(?:a|o|as|os)\s+)?(.+?)\s*,?\s+e\s+(?:(?:a|o)\s+)?(?:centro\s+gestor\s+e\s+operacional\s+do\s+sistema\s+de\s+protecao\s+da\s+amazonia|censipam)",
        opening_normalized,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if before_internal:
        add(before_internal.group(1), "participe_direto", "formula_celebratoria", before_internal.group(0), CONFIDENCE_MEDIUM, "act.partes.formula_invertida")

    # Qualification blocks bind full institutional names to their declared
    # acronym. They outrank a bare celebratory formula.
    qualification_pattern = re.compile(
        r"(?:(?:^|[.;:]\s+|,\s+e\s+)(?:a|o|as|os)?\s*)"
        r"([A-ZÀ-Ý][A-ZÀ-Ý0-9 /&.\-–—\r\n]{5,180}?)"
        r",\s*doravante\s+denominad[oa]\s+(?:(?:como|simplesmente)\s+)?([A-Z0-9]{2,})\b",
        flags=re.IGNORECASE | re.MULTILINE,
    )
    for match in qualification_pattern.finditer(opening):
        name = _clean_party_candidate(match.group(1))
        value = f"{name} - {match.group(2).upper()}"
        add(value, "participe_direto", "qualificacao_inicial", match.group(0), CONFIDENCE_HIGH, "act.partes.qualificacao_doravante")

    obligations_pattern = re.compile(
        r"\b(?:o|a)?\s*censipam\s+e\s+(?:(?:a|o|as|os)\s+)?([^.;:]{4,180}?)\s+(?:ficam|ficarao|obrigam-se)\s+(?:obrigad[oa]s?|a\s+)",
        flags=re.IGNORECASE,
    )
    body_without_object = re.sub(
        r"CL[ÁA]USULA\s+PRIMEIRA.*?(?=CL[ÁA]USULA\s+(?:SEGUNDA|2))",
        "",
        prepared,
        flags=re.IGNORECASE | re.DOTALL,
    )
    for match in obligations_pattern.finditer(body_without_object):
        add(match.group(1), "participe_direto", "clausula_participes", match.group(0), CONFIDENCE_MEDIUM, "act.partes.clausula_obrigacoes")

    # Explicit non-party roles are retained for audit and can never become the
    # singular counterparty merely because they occur near the parties.
    for role, marker in _NON_PARTY_ROLE_PATTERNS:
        role_pattern = re.compile(
            rf"(?:{marker})\s*[:\-–—]?\s*(?:(?:a|o)\s+)?([^.;\n]{{4,180}})",
            flags=re.IGNORECASE,
        )
        for match in role_pattern.finditer(opening):
            add(match.group(1), role, "qualificacao_inicial", match.group(0), CONFIDENCE_HIGH, f"act.partes.{role}")

    # Signature institutions are fallback evidence only. A parenthesized
    # acronym confirms an already linked institution; it does not turn a
    # personal name into an organization.
    signature_start = _normalize_text(prepared).find("documento assinado eletronicamente")
    signature_text = prepared[signature_start:] if signature_start >= 0 else prepared[-2500:]
    signature_siglas = {
        sigla.upper()
        for sigla in re.findall(r"(?:Diretor|Presidente|Secret[aá]ri[oa]|Representante)[^\n]{0,100}\(([A-Z0-9]{2,})\)", signature_text, flags=re.IGNORECASE)
    }
    for candidate in candidates:
        if candidate.get("sigla", "").upper() in signature_siglas:
            candidate["signature_confirmation"] = True
    institutional_signature = re.compile(
        r"(?:Representante|Diretor(?:a)?(?:-Geral)?|Presidente|Secret[aá]ri[oa])\s+d[ao]\s+"
        r"([A-ZÀ-Ý][A-Za-zÀ-ÿ0-9 /&.\-–—]{4,100}?)(?=\n|$)",
        flags=re.IGNORECASE | re.MULTILINE,
    )
    for match in institutional_signature.finditer(signature_text):
        add(match.group(1), "participe_direto", "assinaturas", match.group(0), CONFIDENCE_LOW, "act.partes.assinatura_institucional")

    grouped: Dict[str, Dict[str, Any]] = {}
    for candidate in candidates:
        key = _party_key(candidate["normalized_value"])
        sigla = candidate.get("sigla", "").lower()
        merge_key = sigla or key
        existing = next(
            (
                item
                for item in grouped.values()
                if _party_key(item["normalized_value"]) == key
                or (sigla and str(item.get("sigla", "")).lower() == sigla)
            ),
            None,
        )
        if existing is None:
            candidate["evidences"] = [candidate["evidence"]]
            candidate["zones"] = [candidate["zone"]]
            grouped[merge_key] = candidate
            continue
        existing["evidences"].append(candidate["evidence"])
        if candidate["zone"] not in existing["zones"]:
            existing["zones"].append(candidate["zone"])
        if (
            _PARTY_CONFIDENCE_SCORE[candidate["confidence"]] > _PARTY_CONFIDENCE_SCORE[existing["confidence"]]
            or (candidate.get("institutional_link") and not existing.get("institutional_link"))
        ):
            for field in ("name", "sigla", "normalized_value", "role", "zone", "confidence", "source_rule", "institutional_link"):
                existing[field] = candidate[field]
        existing["signature_confirmation"] = bool(existing.get("signature_confirmation") or candidate.get("signature_confirmation"))

    resolved = list(grouped.values())
    direct = [candidate for candidate in resolved if candidate["role"] == "participe_direto" and not _looks_like_internal_orgao(candidate["normalized_value"])]
    direct.sort(key=lambda item: (-_PARTY_CONFIDENCE_SCORE[item["confidence"]], item["position"]))
    selected: Optional[Dict[str, Any]] = None
    warning = ""
    if direct:
        top_score = _PARTY_CONFIDENCE_SCORE[direct[0]["confidence"]]
        tied = [candidate for candidate in direct if _PARTY_CONFIDENCE_SCORE[candidate["confidence"]] == top_score]
        if len(tied) == 1:
            selected = tied[0]
        elif len({_party_key(candidate["normalized_value"]) for candidate in tied}) == 1:
            selected = tied[0]
        else:
            warning = "multiplos_participes_ambiguos"
            for candidate in tied:
                candidate["rejection_reason"] = warning
    if selected and len(direct) > 1:
        warning = "multiplos_participes_contraparte_priorizada"
        for candidate in direct:
            if candidate is not selected and not candidate.get("rejection_reason"):
                candidate["rejection_reason"] = "menor_confianca_estrutural"

    return {
        "candidates": resolved,
        "internal_party": next((candidate for candidate in resolved if candidate["role"] == "parte_interna"), None),
        "selected_counterparty": selected,
        "warning": warning,
        "source_scope": "snapshot_act_canonico",
    }


def _extract_orgao_convenente(snapshot: Dict[str, Any]) -> Tuple[str, str]:
    extraction = _extract_party_candidates(snapshot)
    selected = extraction.get("selected_counterparty")
    if not selected:
        return ("", "")
    return (str(selected.get("normalized_value", "") or ""), str(selected.get("source_rule", "") or ""))


def _extract_objeto(snapshot: Dict[str, Any]) -> Tuple[str, str]:
    text = str(snapshot.get("text", "") or "")
    section = _extract_section(
        text,
        (
            r"(?:\d+\s*[.)-]\s*)?CL[\u00c1A]USULA\s+PRIMEIRA\s*[-–—]?\s*(?:DO|DA)\s+OBJETO",
            r"(?:\d+\s*[.)-]\s*)?CL[\u00c1A]USULA\s+.*?\s*[-–—]?\s*(?:DO|DA)\s+OBJETO",
            r"\bDO\s+OBJETO\b\s*:",
            r"\bOBJETO\b\s*:",
        ),
    )
    if section:
        cleaned = _clean_clause_value(section)
        return (cleaned, "clausula_objeto") if cleaned else ("", "")

    prepared = _prepare_text(text[:HEADER_SCAN_CHARS])
    match = re.search(r"\bOBJETO\b\s*:\s*(.+)", prepared, flags=re.IGNORECASE)
    if match:
        return (_clean_spaces(match.group(1)), "cabecalho_objeto")
    return ("", "")


def _extract_explicit_named_value(text: str, labels: Tuple[str, ...]) -> str:
    prepared = _prepare_text(text)
    for label in labels:
        pattern = rf"{label}\s*(?:-|–|—|:)\s*([A-Z\u00c0-\u00dd][^\n\r:]+)"
        match = re.search(pattern, prepared, flags=re.IGNORECASE)
        if not match:
            continue
        value = _clean_spaces(match.group(1))
        value = re.split(
            r"\b(?:Substituto|Titular|CPF|RG|Matr[i\u00ed]cula|Suplente|Unidade Respons[a\u00e1]vel)\b",
            value,
            maxsplit=1,
            flags=re.IGNORECASE,
        )[0].strip()
        if _has_content(value, min_alpha=4):
            return value
    return ""


def _extract_gestores(snapshot: Dict[str, Any]) -> Tuple[str, str, str]:
    text = str(snapshot.get("text", "") or "")
    titular = _extract_explicit_named_value(
        text,
        ("Gestor Titular", "Representante Titular", "Fiscal Titular", "Titular", "Gestor"),
    )
    substituto = _extract_explicit_named_value(
        text,
        ("Gestor Substituto", "Representante Substituto", "Fiscal Substituto", "Substituto", "Suplente"),
    )
    source = "rotulos_explicitos" if titular or substituto else ""
    return (titular, substituto, source)


def _extract_unidade_responsavel(snapshot: Dict[str, Any]) -> Tuple[str, str]:
    text = str(snapshot.get("text", "") or "")
    value = _extract_explicit_named_value(
        text,
        (r"Unidade Respons[a\u00e1]vel", r"[AÁ]rea Respons[a\u00e1]vel", r"Unidade Demandante"),
    )
    return (value, "rotulo_unidade_responsavel" if value else "")


def _extract_relatorio_encerramento(snapshot: Dict[str, Any]) -> bool:
    normalized = _normalize_text(str(snapshot.get("text", "") or ""))
    if any(marker in normalized for marker in FINAL_REPORT_MARKERS):
        return True
    if any(marker in normalized for marker in REPORT_MARKERS) and any(
        marker in normalized for marker in FINALIZATION_MARKERS
    ):
        return not any(marker in normalized for marker in PERIODIC_REPORT_MARKERS)
    return False


def _collect_validation_warnings(
    payload: Dict[str, Any],
    analysis: Dict[str, Any],
    vigencia_warning: str,
) -> str:
    warnings: List[str] = []
    alignment_status = _clean_spaces(str(analysis.get("process_alignment_status", "") or ""))
    document_processo = _clean_spaces(str(analysis.get("document_processo", "") or ""))
    if alignment_status == "material_mismatch" and document_processo:
        warnings.append(f"processo_divergente_documento={document_processo}")
    elif alignment_status == "external_reference" and document_processo:
        warnings.append(f"processo_referencia_externa_documento={document_processo}")
    if _clean_spaces(str(analysis.get("classification_reason", "") or "")) == "act_sem_marcador_interno":
        warnings.append("act_sem_marcador_interno")
    if vigencia_warning:
        warnings.append(vigencia_warning)
    payload_processo = _clean_spaces(str(payload.get("processo", "") or ""))
    if not payload_processo and document_processo:
        warnings.append(f"processo_documento_sem_payload={document_processo}")
    return "; ".join(warnings)


def _canonical_score(payload: Dict[str, Any], normalized_record: Dict[str, Any]) -> int:
    if normalized_record.get("doc_class") != DOC_CLASS_ACT_FINAL:
        return -1000

    snapshot = payload.get("snapshot", {}) or {}
    collection = payload.get("collection", {}) or {}
    blobs = _text_blobs(snapshot, collection)
    label_blob = " ".join(part for part in (blobs["normalized_title"], blobs["normalized_selected"]) if part)
    opening_blob = blobs["normalized_opening"]

    score = 0
    if "acordo de cooperacao tecnica" in opening_blob:
        score += 140
    elif "acordo de cooperacao" in opening_blob:
        score += 100

    if "acordo de cooperacao tecnica" in label_blob:
        score += 60
    elif "acordo de cooperacao" in label_blob:
        score += 30

    if any(marker in opening_blob for marker in CONTRACTUAL_MARKERS):
        score += 60
    if "para os fins que especifica" in opening_blob:
        score += 20
    if "assinado" in opening_blob or "assinado" in label_blob:
        score += 10

    if normalized_record.get("numero_acordo"):
        score += 20
    if normalized_record.get("objeto"):
        score += 10
    if normalized_record.get("orgao_convenente"):
        score += 10

    if normalized_record.get("validation_status") != VALIDATION_STATUS_VALID:
        score -= 500
    if not bool(normalized_record.get("has_internal_context")):
        score -= 400
    if normalized_record.get("process_alignment_status") == "material_mismatch":
        score -= 400
    elif normalized_record.get("process_alignment_status") == "external_reference":
        score -= 25

    for marker in TREE_PENALTY_MARKERS:
        if marker in label_blob:
            score -= 20 if marker == "anexo" else 80
    for marker in ("portaria", "publicacao", "reuniao", "plano de trabalho", "termo aditivo", "termo de adesao"):
        if marker in opening_blob or marker in label_blob:
            score -= 180

    score += min(len(str(snapshot.get("text", "") or "")) // 5000, 10)
    return score


def _field_or_missing(
    *,
    value: str,
    raw_value: str = "",
    source_type: str,
    confidence: str,
    rule_id: str,
    warning: str = "",
) -> Dict[str, Any]:
    if not _clean_spaces(str(value or "")):
        return make_missing_field(rule_id=rule_id, warning=warning or "missing")
    return make_field(
        value=value,
        raw_value=raw_value or value,
        source_type=source_type,
        confidence=confidence,
        rule_id=rule_id,
        warning=warning,
    )


def _vigencia_source_type(field_source_vigencia: str) -> str:
    normalized = _normalize_text(field_source_vigencia)
    if "assinatura" in normalized or "publicacao" in normalized or "prazo" in normalized:
        return SOURCE_DERIVED
    if normalized:
        return SOURCE_DOCUMENT_TEXT
    return SOURCE_MISSING


def _build_contract_fields(record: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    vigencia_source = _clean_spaces(str(record.get("field_source_vigencia", "") or ""))
    vigencia_source_type = _vigencia_source_type(vigencia_source)
    vigencia_confidence = CONFIDENCE_MEDIUM if vigencia_source_type == SOURCE_DERIVED else CONFIDENCE_HIGH
    vigencia_warning = _clean_spaces(str(record.get("vigencia_warning", "") or ""))
    numero_field = _field_or_missing(
            value=str(record.get("numero_acordo", "") or ""),
            raw_value=str(record.get("numero_acordo_raw", "") or ""),
            source_type=str(record.get("numero_acordo_source_type", "") or SOURCE_MISSING),
            confidence=str(record.get("numero_acordo_confidence", "") or CONFIDENCE_LOW),
            rule_id=str(record.get("field_source_numero_acordo", "") or "act.numero_acordo.missing"),
            warning=str(record.get("numero_acordo_warning", "") or ""),
        )
    numero_field["evidence"] = str(record.get("numero_acordo_evidence", "") or "")
    numero_field["evidences"] = record.get("numero_acordo_evidences", []) or []
    return {
        "numero_acordo": numero_field,
        "data_assinatura": _field_or_missing(
            value=str(record.get("data_assinatura", "") or ""),
            source_type=SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_MEDIUM,
            rule_id="act.data_assinatura.assinaturas_eletronicas",
        ),
        "data_publicacao": _field_or_missing(
            value=str(record.get("data_publicacao", "") or ""),
            source_type=SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_MEDIUM,
            rule_id="act.data_publicacao.publicacao",
        ),
        "vigencia_raw": _field_or_missing(
            value=str(record.get("vigencia_raw", "") or ""),
            source_type=SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_HIGH,
            rule_id="act.vigencia.raw",
            warning=vigencia_warning,
        ),
        "vigencia_inicio": _field_or_missing(
            value=str(record.get("vigencia_inicio", "") or ""),
            raw_value=str(record.get("vigencia_raw", "") or ""),
            source_type=vigencia_source_type,
            confidence=vigencia_confidence,
            rule_id=vigencia_source or "act.vigencia.missing",
            warning=vigencia_warning,
        ),
        "vigencia_fim": _field_or_missing(
            value=str(record.get("vigencia_fim", "") or ""),
            raw_value=str(record.get("vigencia_raw", "") or ""),
            source_type=vigencia_source_type,
            confidence=vigencia_confidence,
            rule_id=vigencia_source or "act.vigencia.missing",
            warning=vigencia_warning,
        ),
        "data_inicio_vigencia": _field_or_missing(
            value=str(record.get("data_inicio_vigencia", "") or ""),
            raw_value=str(record.get("vigencia_raw", "") or ""),
            source_type=vigencia_source_type,
            confidence=vigencia_confidence,
            rule_id=vigencia_source or "act.vigencia.missing",
            warning=vigencia_warning,
        ),
        "data_fim_vigencia": _field_or_missing(
            value=str(record.get("data_fim_vigencia", "") or ""),
            raw_value=str(record.get("vigencia_raw", "") or ""),
            source_type=vigencia_source_type,
            confidence=vigencia_confidence,
            rule_id=vigencia_source or "act.vigencia.missing",
            warning=vigencia_warning,
        ),
        "orgao_convenente": _field_or_missing(
            value=str(record.get("orgao_convenente", "") or ""),
            source_type=SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_HIGH,
            rule_id="act.orgao_convenente.preambulo",
        ),
        "orgao_convenente_nome": _field_or_missing(
            value=str(record.get("orgao_convenente_nome", "") or ""),
            source_type=SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_HIGH,
            rule_id="act.orgao_convenente_nome.preambulo",
        ),
        "orgao_convenente_sigla": _field_or_missing(
            value=str(record.get("orgao_convenente_sigla", "") or ""),
            source_type=SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_MEDIUM,
            rule_id="act.orgao_convenente_sigla.preambulo",
        ),
        "orgao_intermediario": _field_or_missing(
            value=str(record.get("orgao_intermediario", "") or ""),
            source_type=SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_MEDIUM,
            rule_id="act.orgao_intermediario.preambulo",
        ),
        "objeto": _field_or_missing(
            value=str(record.get("objeto", "") or ""),
            source_type=SOURCE_DOCUMENT_TEXT if record.get("field_source_objeto") else SOURCE_MISSING,
            confidence=CONFIDENCE_HIGH if record.get("field_source_objeto") else CONFIDENCE_LOW,
            rule_id=str(record.get("field_source_objeto", "") or "act.objeto.missing"),
        ),
        "gestor_titular": _field_or_missing(
            value=str(record.get("gestor_titular", "") or ""),
            source_type=SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_MEDIUM,
            rule_id=str(record.get("field_source_gestao", "") or "act.gestor_titular.missing"),
        ),
        "gestor_substituto": _field_or_missing(
            value=str(record.get("gestor_substituto", "") or ""),
            source_type=SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_MEDIUM,
            rule_id=str(record.get("field_source_gestao", "") or "act.gestor_substituto.missing"),
        ),
        "unidade_responsavel": _field_or_missing(
            value=str(record.get("unidade_responsavel", "") or ""),
            source_type=SOURCE_DOCUMENT_TEXT,
            confidence=CONFIDENCE_MEDIUM,
            rule_id=str(record.get("field_source_gestao", "") or "act.unidade_responsavel.missing"),
        ),
    }


def _act_source_kind(source_type: str) -> SourceKind | None:
    if source_type == SOURCE_DERIVED:
        return SourceKind.DERIVED
    if source_type in {SOURCE_DOCUMENT_TEXT, SOURCE_DOCUMENT_TITLE, SOURCE_DOCUMENT_METADATA}:
        return SourceKind.DOCUMENT
    return None


def _act_document_identity(record: Dict[str, Any], payload: Dict[str, Any]) -> DocumentIdentity:
    collection = payload.get("collection", {}) or {}
    snapshot = payload.get("snapshot", {}) or {}
    source_url = _clean_spaces(str(collection.get("source_url") or snapshot.get("url") or "")) or None
    query = parse_qs(urlparse(source_url).query) if source_url else {}
    document_values = query.get("id_documento", ())
    candidate_values = query.get("id_anexo", ())
    document_id = _clean_spaces(str(collection.get("document_id", "") or "")) or None
    candidate_id = _clean_spaces(str(collection.get("candidate_id", "") or "")) or None
    if document_id is None and len(document_values) == 1 and document_values[0].strip().isdigit():
        document_id = document_values[0].strip()
    if candidate_id is None and len(candidate_values) == 1 and candidate_values[0].strip().isdigit():
        candidate_id = candidate_values[0].strip()
    if document_id == _clean_spaces(str(record.get("processo", "") or "")):
        document_id = None
    return DocumentIdentity(
        process_id=record.get("processo", ""),
        document_id=document_id,
        candidate_id=candidate_id,
        source_url=source_url,
    )


def _act_field_evidences(
    field_name: str,
    field: Dict[str, Any],
    *,
    identity: DocumentIdentity,
    source_path: str | None,
    legacy_evidences: Iterable[Dict[str, Any]] = (),
) -> Tuple[FieldEvidence, ...]:
    location = EvidenceLocation(source_path=source_path) if source_path else None
    source_items = list(legacy_evidences)
    if not source_items:
        source_items = [field]

    evidences: List[FieldEvidence] = []
    for item in source_items:
        source_kind = _act_source_kind(str(item.get("source_type", "") or ""))
        if source_kind is None:
            continue
        rule_id = _clean_spaces(str(item.get("rule_id", "") or "")) or None
        raw_evidence = _clean_spaces(
            str(item.get("evidence", "") or item.get("raw_value", "") or "")
        ) or None
        evidence = FieldEvidence(
            field_name=field_name,
            source_kind=source_kind,
            source_document=identity,
            rule_id=rule_id,
            location=location,
            raw_evidence=raw_evidence,
        )
        if evidence not in evidences:
            evidences.append(evidence)
    return tuple(evidences)


def build_act_v2_record(
    record: Dict[str, Any],
    payload: Dict[str, Any],
    *,
    source_path: str | Path | None = None,
) -> Dict[str, Any]:
    """Build additive ACT V2 fields without changing legacy decisions or values."""

    identity = _act_document_identity(record, payload)
    contract = record.get("normalization_contract", {})
    legacy_fields = contract.get("fields", {}) if isinstance(contract, dict) else {}
    resolved_source_path = str(source_path) if source_path else None
    field_results: List[FieldResult] = []
    for field_name, field in legacy_fields.items():
        if not isinstance(field, dict):
            continue
        value = field.get("value")
        present = value is not None and (not isinstance(value, str) or bool(value.strip()))
        evidences = ()
        if present:
            legacy_evidences = record.get("numero_acordo_evidences", []) if field_name == "numero_acordo" else ()
            evidences = _act_field_evidences(
                field_name,
                field,
                identity=identity,
                source_path=resolved_source_path,
                legacy_evidences=legacy_evidences,
            )
        field_results.append(
            FieldResult(
                field_name=field_name,
                state=FieldState.PRESENT if present else FieldState.NOT_EVALUATED,
                value=value if present else None,
                evidences=evidences,
            )
        )

    adapted = adapt_legacy_record(
        {
            **record,
            "process_id": identity.process_id,
            "document_id": identity.document_id,
            "candidate_id": identity.candidate_id,
            "source_url": identity.source_url,
        }
    )
    adapted["fields"] = [field.to_dict() for field in field_results]
    return adapted


def _refresh_contract(record: Dict[str, Any]) -> None:
    existing = record.get("normalization_contract")
    fields = existing.get("fields", {}) if isinstance(existing, dict) else _build_contract_fields(record)
    record["normalization_contract"] = build_document_contract(
        processo=str(record.get("processo", "") or ""),
        requested_type=str(record.get("requested_type", "") or ""),
        resolved_document_type=str(record.get("resolved_document_type", "") or ""),
        documento=None,
        found=True,
        is_canonical_candidate=bool(record.get("is_canonical_candidate")),
        validation_status=str(record.get("validation_status", "") or ""),
        publication_status=str(record.get("publication_status", "") or ""),
        normalization_status=str(record.get("normalization_status", "") or ""),
        fields=fields,
        extra_issues=[
            str(record.get("validation_warning", "") or ""),
            str(record.get("canon_rejection_reason", "") or ""),
        ],
    )


def build_normalized_record(payload: Dict[str, Any], json_path: Path) -> Dict[str, Any]:
    snapshot = payload.get("snapshot", {}) or {}
    collection = payload.get("collection", {}) or {}
    processo = _clean_spaces(str(payload.get("processo", "") or ""))
    requested_type = _clean_spaces(
        str(payload.get("requested_type", "") or str(payload.get("document_type", "") or ""))
    ).lower() or "act"
    analysis = classify_cooperation_snapshot(
        snapshot,
        requested_type,
        collection_context=collection,
        processo=processo,
    )

    numero_acordo = ""
    data_assinatura = ""
    datas_assinatura = ""
    data_publicacao = ""
    vigencia_raw = ""
    data_inicio_vigencia = ""
    data_fim_vigencia = ""
    orgao_convenente = ""
    orgao_convenente_nome = ""
    orgao_convenente_sigla = ""
    orgao_intermediario = ""
    objeto = ""
    gestor_titular = ""
    gestor_substituto = ""
    unidade_responsavel = ""
    field_source_numero_acordo = ""
    field_source_objeto = ""
    field_source_vigencia = ""
    field_source_gestao = ""
    vigencia_warning = ""
    vigencia_rule = {"amount": "", "unit": "", "anchor": ""}
    numero_warning = ""
    numero_source_type = SOURCE_MISSING
    numero_confidence = CONFIDENCE_LOW
    numero_evidence = ""
    numero_evidences: List[Dict[str, Any]] = []
    data_publicacao_warning = ""
    party_extraction: Dict[str, Any] = {
        "candidates": [],
        "internal_party": None,
        "selected_counterparty": None,
        "warning": "",
        "source_scope": "snapshot_act_canonico",
    }

    if analysis.get("doc_class") == DOC_CLASS_ACT_FINAL:
        if analysis.get("is_canonical_candidate"):
            numero_result = _extract_numero_acordo(snapshot, collection)
            numero_acordo = str(numero_result.get("value", "") or "")
            field_source_numero_acordo = str(numero_result.get("field_source", "") or "")
            numero_source_type = str(numero_result.get("source_type", "") or SOURCE_MISSING)
            numero_confidence = str(numero_result.get("confidence", "") or CONFIDENCE_LOW)
            numero_evidence = str(numero_result.get("evidence", "") or "")
            numero_evidences = list(numero_result.get("evidences", []) or [])
            numero_warning = str(numero_result.get("warning", "") or "")
        signature_dates = _extract_signature_dates(str(snapshot.get("text", "") or ""))
        data_assinatura = max(signature_dates) if signature_dates else ""
        datas_assinatura = " | ".join(signature_dates)
        publicacao_info = _extract_data_publicacao_info(snapshot, data_assinatura=data_assinatura)
        data_publicacao = publicacao_info["data_publicacao"]
        data_publicacao_warning = publicacao_info["warning"]
        vigencia_raw = _extract_vigencia_raw(snapshot)
        resolved_vigencia = resolve_act_vigencia(
            vigencia_raw,
            data_assinatura=data_assinatura,
            data_publicacao=data_publicacao,
            outras_datas=signature_dates,
        )
        data_inicio_vigencia = resolved_vigencia["vigencia_inicio"]
        data_fim_vigencia = resolved_vigencia["vigencia_fim"]
        field_source_vigencia = resolved_vigencia["field_source_vigencia"]
        vigencia_warning = resolved_vigencia["warning"]
        vigencia_rule = {
            "amount": resolved_vigencia["amount"],
            "unit": resolved_vigencia["unit"],
            "anchor": resolved_vigencia["anchor"],
        }
        party_extraction = _extract_party_candidates(snapshot)
        selected_party = party_extraction.get("selected_counterparty") or {}
        orgao_raw = str(selected_party.get("normalized_value", "") or "")
        orgao_parts = _split_orgao_candidate(orgao_raw) if orgao_raw else {}
        orgao_convenente = orgao_parts.get("orgao_convenente", "")
        orgao_convenente_nome = orgao_parts.get("orgao_convenente_nome", "")
        orgao_convenente_sigla = orgao_parts.get("orgao_convenente_sigla", "")
        orgao_intermediario = orgao_parts.get("orgao_intermediario", "") or _extract_orgao_intermediario(snapshot)
        objeto, field_source_objeto = _extract_objeto(snapshot)
        gestor_titular, gestor_substituto, gestor_source = _extract_gestores(snapshot)
        unidade_responsavel, unidade_source = _extract_unidade_responsavel(snapshot)
        field_source_gestao = gestor_source or unidade_source

    validation_warning = _collect_validation_warnings(payload, analysis, vigencia_warning)
    if data_publicacao_warning:
        validation_warning = "; ".join(part for part in (validation_warning, data_publicacao_warning) if part)
    if numero_warning:
        validation_warning = "; ".join(part for part in (validation_warning, numero_warning) if part)
    party_warning = str(party_extraction.get("warning", "") or "")
    if party_warning:
        validation_warning = "; ".join(part for part in (validation_warning, party_warning) if part)
    document_processos = analysis.get("document_processos", []) or []
    process_affinity = analysis.get("process_affinity", {}) or {}
    record = {
        "requested_type": requested_type,
        "numero_acordo": numero_acordo,
        "processo": processo,
        "data_assinatura": data_assinatura,
        "datas_assinatura": datas_assinatura,
        "data_publicacao": data_publicacao,
        "vigencia_raw": vigencia_raw,
        "vigencia_inicio": data_inicio_vigencia,
        "vigencia_fim": data_fim_vigencia,
        "data_inicio_vigencia": data_inicio_vigencia,
        "data_fim_vigencia": data_fim_vigencia,
        "orgao_convenente": orgao_convenente,
        "orgao_convenente_nome": orgao_convenente_nome,
        "orgao_convenente_sigla": orgao_convenente_sigla,
        "orgao_intermediario": orgao_intermediario,
        "party_extraction": party_extraction,
        "objeto": objeto,
        "gestor_titular": gestor_titular,
        "gestor_substituto": gestor_substituto,
        "unidade_responsavel": unidade_responsavel,
        "classificacao": DOC_CLASS_ACT_FINAL if analysis.get("doc_class") == DOC_CLASS_ACT_FINAL else "",
        "relatorio_encerramento": bool(_extract_relatorio_encerramento(snapshot))
        if analysis.get("doc_class") == DOC_CLASS_ACT_FINAL
        else False,
        "doc_class": analysis.get("doc_class", ""),
        "resolved_document_type": analysis.get("resolved_document_type", ""),
        "is_canonical_candidate": bool(analysis.get("is_canonical_candidate")),
        "validation_status": analysis.get("validation_status", ""),
        "publication_status": analysis.get("publication_status", ""),
        "normalization_status": analysis.get("normalization_status", ""),
        "discard_reason": analysis.get("discard_reason", ""),
        "classification_reason": analysis.get("classification_reason", ""),
        "canon_rejection_reason": ""
        if analysis.get("publication_status") == PUBLICATION_STATUS_GOLD
        else (analysis.get("classification_reason", "") or analysis.get("discard_reason", "")),
        "field_source_numero_acordo": field_source_numero_acordo,
        "numero_acordo_raw": numero_acordo,
        "numero_acordo_source_type": numero_source_type,
        "numero_acordo_confidence": numero_confidence,
        "numero_acordo_evidence": numero_evidence,
        "numero_acordo_evidences": numero_evidences,
        "numero_acordo_warning": numero_warning,
        "field_source_objeto": field_source_objeto,
        "field_source_vigencia": field_source_vigencia,
        "field_source_gestao": field_source_gestao,
        "vigencia_rule_amount": vigencia_rule.get("amount", ""),
        "vigencia_rule_unit": vigencia_rule.get("unit", ""),
        "vigencia_rule_anchor": vigencia_rule.get("anchor", ""),
        "vigencia_warning": vigencia_warning,
        "validation_warning": validation_warning,
        "has_internal_context": bool(analysis.get("has_internal_context")),
        "process_alignment_status": analysis.get("process_alignment_status", ""),
        "document_processo": analysis.get("document_processo", ""),
        "document_processos": " | ".join(document_processos),
        "process_affinity": process_affinity,
        "current_process_explicit": json.dumps(
            process_affinity.get("current_process_explicit", {}), ensure_ascii=False
        ),
        "current_process_in_metadata": json.dumps(
            process_affinity.get("current_process_in_metadata", {}), ensure_ascii=False
        ),
        "external_processes_found": json.dumps(
            process_affinity.get("external_processes_found", []), ensure_ascii=False
        ),
        "document_origin_process": str(
            (process_affinity.get("document_origin_process", {}) or {}).get("process", "")
        ),
        "document_origin_source": str(
            (process_affinity.get("document_origin_process", {}) or {}).get("source", "")
        ),
        "document_origin_confidence": str(
            (process_affinity.get("document_origin_process", {}) or {}).get("confidence", "")
        ),
        "affinity_status": str(process_affinity.get("affinity_status", "")),
        "affinity_confidence": str(process_affinity.get("affinity_confidence", "")),
        "affinity_evidence": " | ".join(process_affinity.get("affinity_evidence", []) or []),
        "affinity_rule_version": str(process_affinity.get("affinity_rule_version", "")),
        "snapshot_mode": _clean_spaces(str(snapshot.get("extraction_mode", "") or "")),
        "text_chars": len(str(snapshot.get("text", "") or "")),
        "candidate_json_path": str(json_path),
        "json_path": str(json_path),
        "canonical_score": 0,
    }
    record["canonical_score"] = _canonical_score(payload, record)
    record["normalization_contract"] = build_document_contract(
        processo=record["processo"],
        requested_type=record["requested_type"],
        resolved_document_type=record["resolved_document_type"],
        documento=_clean_spaces(str(payload.get("documento", "") or "")) or None,
        found=True,
        is_canonical_candidate=bool(record["is_canonical_candidate"]),
        validation_status=record["validation_status"],
        publication_status=record["publication_status"],
        normalization_status=record["normalization_status"],
        fields=_build_contract_fields(record),
        extra_issues=[
            record.get("validation_warning", ""),
            record.get("canon_rejection_reason", ""),
        ],
    )
    return record


def _build_field_diagnostics(records: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    diagnostics: List[Dict[str, str]] = []
    for record in records:
        contract = record.get("normalization_contract", {})
        fields = contract.get("fields", {}) if isinstance(contract, dict) else {}
        processo = _clean_spaces(str(record.get("processo", "") or ""))
        for campo, field in fields.items():
            if not isinstance(field, dict):
                continue
            diagnostics.append(
                {
                    "processo": processo,
                    "campo": str(campo),
                    "valor": _clean_spaces(str(field.get("value", "") or "")),
                    "raw_value": _clean_spaces(str(field.get("raw_value", "") or "")),
                    "source_type": _clean_spaces(str(field.get("source_type", "") or "")),
                    "confidence": _clean_spaces(str(field.get("confidence", "") or "")),
                    "warning": _clean_spaces(str(field.get("warning", "") or "")),
                    "rule_id": _clean_spaces(str(field.get("rule_id", "") or "")),
                    "evidence": _clean_spaces(str(field.get("evidence", "") or "")),
                    "evidences": json.dumps(field.get("evidences", []) or [], ensure_ascii=False),
                }
            )
    return diagnostics


def export_normalized_csv(output_dir: Path, logger: Any = None) -> Dict[str, Any]:
    csv_writer.ensure_output_dir(output_dir)
    json_paths = _collect_act_snapshot_paths(output_dir)
    if not json_paths:
        _log(logger, "info", "Normalizador ACT: nenhum JSON encontrado em %s.", output_dir)
        return {"records": 0, "csv_path": None, "audit_path": None}

    audit_records: List[Dict[str, Any]] = []
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    payloads_by_record: Dict[int, Dict[str, Any]] = {}
    for json_path in json_paths:
        try:
            payload = _read_json(json_path)
            record = build_normalized_record(payload, json_path)
            payloads_by_record[id(record)] = payload
            grouped.setdefault(record["processo"], []).append(record)
            audit_records.append(record)
        except Exception as exc:
            _log(logger, "warning", "Normalizador ACT: falha ao processar %s (%s).", json_path, exc)

    canonical_records: List[Dict[str, Any]] = []
    for processo, records in grouped.items():
        canonical_candidates = [
            record
            for record in records
            if record.get("doc_class") == DOC_CLASS_ACT_FINAL
            and record.get("validation_status") == VALIDATION_STATUS_VALID
        ]
        if not canonical_candidates:
            alias_path = _act_alias_path(output_dir, processo)
            if alias_path.exists() and any(Path(str(record.get("json_path", ""))).parent.name == "candidates" for record in records):
                try:
                    alias_path.unlink()
                except OSError as exc:
                    _log(logger, "warning", "Normalizador ACT: falha ao remover alias nao canonico %s (%s).", alias_path, exc)
            for record in records:
                record["normalization_status"] = "descartado_nao_canonico"
                record["publication_status"] = PUBLICATION_STATUS_SILVER
                if not record.get("discard_reason"):
                    record["discard_reason"] = record.get("doc_class", "")
                if not record.get("canon_rejection_reason"):
                    record["canon_rejection_reason"] = record.get("classification_reason", "") or record.get(
                        "discard_reason",
                        "",
                    )
                _refresh_contract(record)
            continue

        canonical = max(
            canonical_candidates,
            key=lambda item: (
                int(item.get("canonical_score", 0) or 0),
                int(item.get("text_chars", 0) or 0),
                len(item.get("objeto", "")),
            ),
        )
        for record in records:
            if record is canonical:
                source_path = Path(str(record.get("candidate_json_path", "") or record.get("json_path", "")))
                alias_path = _publish_act_alias(
                    output_dir,
                    source_path,
                    processo,
                    party_extraction=record.get("party_extraction", {}),
                    process_affinity=record.get("process_affinity", {}),
                )
                record["json_path"] = str(alias_path)
                record["normalization_status"] = "publicado_canonico"
                record["publication_status"] = PUBLICATION_STATUS_GOLD
                record["discard_reason"] = ""
                record["canon_rejection_reason"] = ""
                _refresh_contract(record)
                canonical_records.append(record)
            elif record.get("doc_class") == DOC_CLASS_ACT_FINAL and record.get("validation_status") == VALIDATION_STATUS_VALID:
                record["normalization_status"] = "descartado_por_desempate"
                record["publication_status"] = PUBLICATION_STATUS_SILVER
                record["discard_reason"] = "act_final_nao_canonico"
                record["canon_rejection_reason"] = "act_final_nao_canonico"
                _refresh_contract(record)
            else:
                record["normalization_status"] = "descartado_nao_canonico"
                record["publication_status"] = PUBLICATION_STATUS_SILVER
                if not record.get("discard_reason"):
                    record["discard_reason"] = record.get("doc_class", "")
                if not record.get("canon_rejection_reason"):
                    record["canon_rejection_reason"] = record.get("classification_reason", "") or record.get(
                        "discard_reason",
                        "",
                    )
                _refresh_contract(record)
        _log(logger, "info", "Normalizador ACT: processo %s canonico=%s.", processo, canonical.get("json_path", ""))

    audit_columns = [
        "requested_type",
        "processo",
        "numero_acordo",
        "doc_class",
        "resolved_document_type",
        "is_canonical_candidate",
        "validation_status",
        "publication_status",
        "normalization_status",
        "discard_reason",
        "classification_reason",
        "canon_rejection_reason",
        "data_assinatura",
        "datas_assinatura",
        "data_publicacao",
        "vigencia_raw",
        "vigencia_inicio",
        "vigencia_fim",
        "data_inicio_vigencia",
        "data_fim_vigencia",
        "orgao_convenente",
        "orgao_convenente_nome",
        "orgao_convenente_sigla",
        "orgao_intermediario",
        "objeto",
        "gestor_titular",
        "gestor_substituto",
        "unidade_responsavel",
        "relatorio_encerramento",
        "field_source_numero_acordo",
        "numero_acordo_source_type",
        "numero_acordo_confidence",
        "numero_acordo_evidence",
        "numero_acordo_evidences",
        "numero_acordo_warning",
        "field_source_objeto",
        "field_source_vigencia",
        "field_source_gestao",
        "vigencia_rule_amount",
        "vigencia_rule_unit",
        "vigencia_rule_anchor",
        "vigencia_warning",
        "validation_warning",
        "has_internal_context",
        "process_alignment_status",
        "document_processo",
        "document_processos",
        "current_process_explicit",
        "current_process_in_metadata",
        "external_processes_found",
        "document_origin_process",
        "document_origin_source",
        "document_origin_confidence",
        "affinity_status",
        "affinity_confidence",
        "affinity_evidence",
        "affinity_rule_version",
        "snapshot_mode",
        "text_chars",
        "canonical_score",
        "candidate_json_path",
        "json_path",
    ]
    audit_path = output_dir / "act_classificacao_latest.csv"
    csv_writer.write_csv(audit_records, audit_path, columns=audit_columns)

    affinity_columns = [
        "processo",
        "candidate_json_path",
        "publication_status",
        "is_canonical_candidate",
        "process_alignment_status",
        "affinity_status",
        "affinity_confidence",
        "document_origin_process",
        "document_origin_source",
        "current_process_explicit",
        "current_process_in_metadata",
        "external_processes_found",
        "affinity_evidence",
        "affinity_rule_version",
        "shadow_only",
        "shadow_review_required",
    ]
    affinity_rows = []
    for row in audit_records:
        affinity_status = str(row.get("affinity_status", "") or "")
        affinity_rows.append(
            {
                **{column: row.get(column, "") for column in affinity_columns},
                "shadow_only": True,
                "shadow_review_required": row.get("publication_status") == PUBLICATION_STATUS_GOLD
                and affinity_status in {"related_document", "ambiguous", "probable_external_document"},
            }
        )
    affinity_path = output_dir / "act_process_affinity_shadow_latest.csv"
    csv_writer.write_csv(affinity_rows, affinity_path, columns=affinity_columns)

    normalized_columns = [
        "numero_acordo",
        "processo",
        "data_assinatura",
        "datas_assinatura",
        "data_publicacao",
        "vigencia_raw",
        "vigencia_inicio",
        "vigencia_fim",
        "data_inicio_vigencia",
        "data_fim_vigencia",
        "orgao_convenente",
        "orgao_convenente_nome",
        "orgao_convenente_sigla",
        "orgao_intermediario",
        "objeto",
        "gestor_titular",
        "gestor_substituto",
        "unidade_responsavel",
        "classificacao",
        "relatorio_encerramento",
    ]
    csv_path = output_dir / "act_normalizado_latest.csv"
    public_rows = [{column: row.get(column, "") for column in normalized_columns} for row in canonical_records]
    csv_writer.write_csv(public_rows, csv_path, columns=normalized_columns)
    v2_path = None
    if get_settings().v2_dual_write:
        envelope = {
            "schema_version": V2_SCHEMA_VERSION,
            "legacy_artifact": csv_path.name,
            "records": [
                build_act_v2_record(
                    record,
                    payloads_by_record[id(record)],
                    source_path=Path(str(record.get("candidate_json_path", ""))).relative_to(output_dir).as_posix(),
                )
                for record in canonical_records
            ],
        }
        v2_path = write_v2_sidecar(v2_sidecar_path(csv_path), envelope)
    diagnostic_columns = [
        "processo", "campo", "valor", "raw_value", "source_type", "confidence",
        "rule_id", "evidence", "evidences", "warning",
    ]
    diagnostics_path = output_dir / "act_field_diagnostics_latest.csv"
    csv_writer.write_csv(_build_field_diagnostics(audit_records), diagnostics_path, columns=diagnostic_columns)
    _log(
        logger,
        "info",
        "Normalizador ACT: CSV canonico gerado com %d registro(s), auditoria=%d.",
        len(public_rows),
        len(audit_records),
    )
    return {
        "records": len(public_rows),
        "csv_path": csv_path,
        "latest_path": csv_path,
        "audit_path": audit_path,
        "affinity_path": affinity_path,
        "diagnostics_path": diagnostics_path,
        "v2_path": v2_path,
    }
