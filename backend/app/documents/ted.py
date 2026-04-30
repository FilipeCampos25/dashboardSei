from __future__ import annotations

from app.documents.cooperation_common import CooperationDocumentHandler
from app.documents.types import DocumentTypeSpec
from app.services.act_normalizer import DOC_CLASS_TED

TED_FILTER_TYPE_ALIASES = (
    "Termo de Execução Descentralizada",
    "Termo de Execucao Descentralizada",
    "TED - Termo de Execução Descentralizada",
    "TED - Termo de Execucao Descentralizada",
    "TED",
)

TED_SEARCH_TERMS = TED_FILTER_TYPE_ALIASES

TED_TREE_MATCH_TERMS = (
    "ted - termo de execução descentralizada",
    "ted - termo de execucao descentralizada",
    "termo de execução descentralizada",
    "termo de execucao descentralizada",
)


def build_ted_document_type() -> DocumentTypeSpec:
    return DocumentTypeSpec(
        key="ted",
        display_name="Termo de Execucao Descentralizada",
        search_terms=TED_SEARCH_TERMS,
        tree_match_terms=TED_TREE_MATCH_TERMS,
        snapshot_prefix="termo_execucao_descentralizada",
        log_label="TED",
        cleanup_patterns=(
            "termo_execucao_descentralizada_*.json",
            "ted_status_execucao_latest.csv",
            "ted_normalizado_latest.csv",
        ),
        handler=CooperationDocumentHandler(
            status_filename="ted_status_execucao_latest.csv",
        ),
        accepted_doc_classes=(DOC_CLASS_TED,),
        filter_type_aliases=TED_FILTER_TYPE_ALIASES,
    )
