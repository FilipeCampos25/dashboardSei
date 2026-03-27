from __future__ import annotations

from app.documents.cooperation_common import CooperationDocumentHandler
from app.documents.types import DocumentTypeSpec
from app.services.act_normalizer import DOC_CLASS_MEMORANDO

MEMORANDO_SEARCH_TERMS = (
    "MEMORANDO DE ENTENDIMENTOS",
    "Memorando de Entendimentos",
)

MEMORANDO_TREE_MATCH_TERMS = ("memorando de entendimentos",)


def build_memorando_document_type() -> DocumentTypeSpec:
    return DocumentTypeSpec(
        key="memorando",
        display_name="Memorando de Entendimentos",
        search_terms=MEMORANDO_SEARCH_TERMS,
        tree_match_terms=MEMORANDO_TREE_MATCH_TERMS,
        snapshot_prefix="memorando_entendimentos",
        log_label="MEMORANDO",
        cleanup_patterns=(
            "memorando_entendimentos_*.json",
            "memorando_status_execucao_latest.csv",
            "memorando_normalizado_latest.csv",
        ),
        handler=CooperationDocumentHandler(
            status_filename="memorando_status_execucao_latest.csv",
        ),
        accepted_doc_classes=(DOC_CLASS_MEMORANDO,),
        filter_type_aliases=("Memorando de Entendimentos",),
    )
