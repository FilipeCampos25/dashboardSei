"""
app/services/selectors.py

Este arquivo NÃO existia no seu zip, mas o seu scraping.py importava:
    from app.services.selectors import load_selectors

Sem isso, depois que você corrigisse o get_settings, o próximo erro seria:
    ModuleNotFoundError: No module named 'app.services.selectors'

Regra respeitada:
- Não otimiza o scraper.
- Só cria o que o scraper já esperava existir: uma função load_selectors()
  que carrega o JSON existente em backend/app/rpa/xpath_selector.json
- Não adiciona bibliotecas novas (usa apenas json/pathlib).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict


def load_selectors() -> Dict[str, Any]:
    """
    Carrega os seletores/XPaths do arquivo JSON do projeto.

    Local esperado (já existe no seu zip):
    - backend/app/rpa/xpath_selector.json

    Por que este caminho:
    - Este arquivo (selectors.py) fica em: backend/app/services/selectors.py
    - Então subimos um nível (app/), entramos em rpa/ e lemos o JSON.

    Se o arquivo não existir, levantamos erro claro, porque sem seletores o scraping não funciona.
    """
    here = Path(__file__).resolve()

    # backend/app/services/selectors.py
    # -> parent = backend/app/services
    # -> parent.parent = backend/app
    # -> backend/app/rpa/xpath_selector.json
    json_path = here.parent.parent / "rpa" / "xpath_selector.json"

    if not json_path.exists():
        raise FileNotFoundError(
            f"Arquivo de seletores não encontrado: {json_path} "
            "(esperado: backend/app/rpa/xpath_selector.json)"
        )

    with json_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # Mantemos a estrutura como dict exatamente como está no JSON
    return data
