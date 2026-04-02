from __future__ import annotations

from app.documents.cooperation_common import CooperationDocumentHandler
from app.documents.types import DocumentTypeSpec
from app.services.act_normalizer import DOC_CLASS_ACT_FINAL

ACT_SEARCH_TERMS = (
    "ACORDO DE COOPERACAO TECNICA - ACT",
    "ACORDO DE COOPERAÇÃO TÉCNICA - ACT",
    "ACORDO DE COOPERACAO TECNICA",
    "ACORDO DE COOPERAÇÃO TÉCNICA",
    "Acordo de Cooperacao Tecnica - ACT",
    "Acordo de Cooperação Técnica - ACT",
    "Acordo de Cooperacao Tecnica",
    "Acordo de Cooperação Técnica",
)

ACT_TREE_MATCH_TERMS = (
    "acordo de cooperacao tecnica - act",
    "acordo de cooperação técnica - act",
    "acordo de cooperacao tecnica",
    "acordo de cooperação técnica",
    "act",
)


def build_act_document_type() -> DocumentTypeSpec:
    return DocumentTypeSpec(
        key="act",
        display_name="Acordo de Cooperacao Tecnica",
        search_terms=ACT_SEARCH_TERMS,
        tree_match_terms=ACT_TREE_MATCH_TERMS,
        snapshot_prefix="acordo_cooperacao_tecnica",
        log_label="ACT",
        cleanup_patterns=(
            "acordo_cooperacao_tecnica_*.json",
            "act_status_execucao_latest.csv",
            "act_normalizado_latest.csv",
            "act_classificacao_latest.csv",
        ),
        handler=CooperationDocumentHandler(
            status_filename="act_status_execucao_latest.csv",
            export_act_normalized=True,
        ),
        accepted_doc_classes=(DOC_CLASS_ACT_FINAL,),
        filter_type_aliases=(
            "Acordo de Cooperação Técnica",
            "Acordo de Cooperação",
        ),
        max_filter_candidates=2,
    )
