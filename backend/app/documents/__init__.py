from __future__ import annotations

from app.documents.types import DocumentTypeHandler, DocumentTypeSpec

__all__ = ["DocumentTypeHandler", "DocumentTypeSpec", "resolve_document_types"]


def __getattr__(name: str):
    if name == "resolve_document_types":
        from app.documents.registry import resolve_document_types

        return resolve_document_types
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
