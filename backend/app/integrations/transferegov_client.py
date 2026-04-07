from __future__ import annotations

import logging
from typing import Any

import requests


LOGGER = logging.getLogger(__name__)

_CONSULTAR_URL = (
    "https://val-siconv.np.estaleiro.serpro.gov.br/maisbrasil-api/v1/"
    "services/public/processo-compra/consultar"
)


def normalize_processo_sei(numero_processo_sei: str) -> tuple[str, int]:
    numero_processo = "".join(char for char in numero_processo_sei if char.isdigit())

    if len(numero_processo) < 4:
        raise ValueError("Numero de processo SEI invalido para extrair o ano.")

    ano = int(numero_processo[-6:-2])
    return numero_processo, ano


def consultar_ted(
    numero_processo: str,
    numero_instrumento: str,
    ano: int,
) -> list[Any] | None:
    params = {
        "numeroProcesso": numero_processo,
        "numeroInstrumento": numero_instrumento,
        "anoInstrumento": ano,
        "anoProcesso": ano,
    }

    try:
        response = requests.get(_CONSULTAR_URL, params=params, timeout=10)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, ValueError) as exc:
        LOGGER.warning("Falha ao consultar TED no Transferegov: %s", exc)
        return None

    if not isinstance(payload, list) or not payload:
        return None

    return payload
