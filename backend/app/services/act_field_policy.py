"""Conservative ACT field-complement policy keyed by document function."""

from __future__ import annotations


_AUTHORIZED_COMPLEMENTS = {
    "act.extract": frozenset({"data_publicacao"}),
}


def may_complement_act(source_function: str | None, field_name: str) -> bool:
    """Return whether current extractors prove this source-to-field relation."""

    return field_name in _AUTHORIZED_COMPLEMENTS.get(source_function or "", ())
