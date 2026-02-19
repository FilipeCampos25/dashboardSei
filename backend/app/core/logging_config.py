"""
app/core/logging_config.py

Este módulo centraliza a configuração de logging do projeto.

Problema que corrige:
- O scraper importava `setup_logger`, mas este arquivo expunha `setup_logging`.
- Isso gera:
    ImportError: cannot import name 'setup_logger' ... Did you mean: 'setup_logging'?

Solução mínima e segura:
- NÃO removemos `setup_logging` (mantemos compatibilidade com o resto do projeto).
- ADICIONAMOS `setup_logger()` como "alias/wrapper" que chama `setup_logging()`.

Regras respeitadas:
1) não quebre nada: adiciona função compatível sem mudar fluxo.
2) não exclua nenhuma função importante: mantemos `setup_logging`.
3) não tente otimizar o código: implementação direta e simples.
4) não use biblioteca nova: usa somente stdlib.
5) respeite fluxo e funcionamento: apenas corrige nome esperado no import.
"""

from __future__ import annotations

import logging
import os
from typing import Optional


def setup_logging(
    level: Optional[str] = None,
    logger_name: str = "dashboard_sei",
) -> logging.Logger:
    """
    Configura o logging global do projeto e retorna um logger.

    - level: nível do log (ex: "INFO", "DEBUG"). Se None:
        1) tenta ler do ambiente: LOG_LEVEL
        2) fallback: "INFO"
    - logger_name: nome do logger base do projeto.

    Observação importante:
    - `basicConfig` só aplica configuração uma vez por processo, então
      chamar isso mais de uma vez não "quebra" (mantido).
    """

    # Define o nível:
    # - Se level for passado, usa ele
    # - Senão, tenta env LOG_LEVEL
    # - Senão, INFO
    if level is None:
        level = os.getenv("LOG_LEVEL", "INFO")

    # Converte para nível do logging (INFO, DEBUG, etc.)
    numeric_level = getattr(logging, level.upper(), logging.INFO)

    # Configuração básica do logging.
    # (Formato simples, suficiente para debug; não estamos "otimizando")
    logging.basicConfig(
        level=numeric_level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    # Retorna um logger nomeado
    return logging.getLogger(logger_name)


def setup_logger() -> logging.Logger:
    """
    Alias de compatibilidade.

    Motivo:
    - Alguns arquivos (ex.: app/rpa/scraping.py) importam `setup_logger`
      mas o nome original do módulo é `setup_logging`.

    O que faz:
    - Apenas chama `setup_logging()` sem parâmetros, respeitando LOG_LEVEL no env.

    Importante:
    - NÃO remove nem altera `setup_logging`.
    - Não muda o fluxo do seu código, só resolve o ImportError.
    """
    return setup_logging()
