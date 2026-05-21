from __future__ import annotations

from app.documents.cooperation_common import CooperationDocumentHandler
from app.documents.types import DocumentTypeSpec
from app.services.act_normalizer import ADMINISTRATIVE_DOC_CLASSES

MEMORANDO_SEARCH_TERMS = (
    "memorando",
    "memo",
    "ofício",
    "oficio",
    "despacho",
    "encaminhamento",
    "informação técnica",
    "informacao tecnica",
    "nota técnica",
    "nota tecnica",
    "solicitação",
    "solicitacao",
)

MEMORANDO_TREE_MATCH_TERMS = MEMORANDO_SEARCH_TERMS


def build_memorando_document_type() -> DocumentTypeSpec:
    return DocumentTypeSpec(
        key="memorando",
        display_name="Documento Administrativo",
        search_terms=MEMORANDO_SEARCH_TERMS,
        tree_match_terms=MEMORANDO_TREE_MATCH_TERMS,
        snapshot_prefix="memorando_entendimentos",
        log_label="DOCUMENTO_ADMINISTRATIVO",
        cleanup_patterns=(
            "memorando_entendimentos_*.json",
            "documento_administrativo_status_execucao_latest.csv",
            "documento_administrativo_normalizado_latest.csv",
            "memorando_status_execucao_latest.csv",
            "memorando_normalizado_latest.csv",
        ),
        handler=CooperationDocumentHandler(
            status_filename="documento_administrativo_status_execucao_latest.csv",
        ),
        accepted_doc_classes=ADMINISTRATIVE_DOC_CLASSES,
        filter_type_aliases=MEMORANDO_SEARCH_TERMS,
        max_filter_candidates=3,
    )
