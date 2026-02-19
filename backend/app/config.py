"""
app/config.py
Centraliza carregamento de variáveis de ambiente (.env) e expõe um objeto Settings.

O que esta alteração resolve (do seu erro atual):
- O seu main.py faz: `from app.config import get_settings`
  mas no arquivo original não existia get_settings() -> causava ImportError.

O que esta alteração também corrige:
- Pydantic v2: a chave `Config.fields` foi removida.
  No original isso gerava WARNING e pode causar comportamento inesperado.
  Aqui trocamos para o padrão V2, mantendo o MESMO mapeamento de variáveis.

Regras respeitadas:
- Não muda fluxo do scraper.
- Não otimiza lógica.
- Não remove funções importantes (apenas adiciona get_settings e ajusta a config do Pydantic).
- Não adiciona bibliotecas novas (usa apenas o que já está no projeto: pydantic/pydantic_settings/dotenv).
"""

from __future__ import annotations

from pathlib import Path

# pydantic v2 / pydantic-settings v2
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

# já estava no seu arquivo original
from dotenv import load_dotenv, find_dotenv


def _load_env() -> None:
    """
    Carrega o .env de forma robusta.

    Estratégia (mantida):
    1) Tenta `find_dotenv()` (procura subindo a partir do CWD).
    2) Se não achar, tenta procurar subindo a partir do arquivo atual.
    3) Se ainda não achar, não falha aqui: o erro aparecerá onde a variável for obrigatória.

    Observação:
    - Isso ajuda quando você roda `python main.py` dentro de `./backend`,
      mas o `.env` está em outro nível (raiz do projeto, etc.).
    """
    # 1) A partir do diretório de execução
    env_path = find_dotenv(filename=".env", usecwd=True)
    if env_path:
        load_dotenv(env_path, override=False)
        return

    # 2) A partir do local deste arquivo: backend/app/config.py -> sobe diretórios
    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        candidate = parent / ".env"
        if candidate.exists():
            load_dotenv(str(candidate), override=False)
            return

    # 3) Nada encontrado: segue sem crash (validação acontecerá no uso)
    return


# Carrega .env assim que o módulo é importado (mantido)
_load_env()


class Settings(BaseSettings):
    """
    Config do projeto.

    IMPORTANTE:
    - No original, você usava class Config + fields = {...}
      Isso é estilo Pydantic v1 e gera warning no v2.
    - Aqui mudamos para o padrão v2 (Field + AliasChoices),
      mantendo o MESMO objetivo: aceitar várias chaves de ambiente.

    Exemplo de chaves aceitas (compatibilidade):
    - sei_url: url_sei / URL / SEI_URL / URL_SEI
    - username: username / USERNAME / USER
    - password: password / PASSWORD / PASS
    """

    # Config do BaseSettings no padrão V2.
    # - extra="ignore": se existir variável no .env que não está no model, não quebra.
    model_config = SettingsConfigDict(extra="ignore")

    # =============================
    # URL do SEI
    # =============================
    sei_url: str | None = Field(
        default=None,
        # Aceita várias variáveis para a mesma config (equivalente ao seu fields/env antigo)
        validation_alias=AliasChoices("url_sei", "URL", "SEI_URL", "URL_SEI"),
    )

    # =============================
    # Credenciais
    # =============================
    username: str | None = Field(
        default=None,
        validation_alias=AliasChoices("username", "USERNAME", "USER"),
    )
    password: str | None = Field(
        default=None,
        validation_alias=AliasChoices("password", "PASSWORD", "PASS"),
    )

    # =============================
    # Execução
    # =============================
    headless: bool = Field(
        default=False,
        validation_alias=AliasChoices("HEADLESS", "headless"),
    )
    timeout_seconds: int = Field(
        default=20,
        validation_alias=AliasChoices("TIMEOUT_SECONDS", "timeout_seconds"),
    )

    # =============================
    # Saída/relatórios
    # =============================
    output_dir: str = Field(
        default="output",
        validation_alias=AliasChoices("OUTPUT_DIR", "output_dir"),
    )
    report_name: str = Field(
        default="report.json",
        validation_alias=AliasChoices("REPORT_NAME", "report_name"),
    )

    # =============================
    # Logging
    # =============================
    log_level: str = Field(
        default="INFO",
        validation_alias=AliasChoices("LOG_LEVEL", "log_level"),
    )


# Instância única (mantido)
settings = Settings()


def get_settings() -> Settings:
    """
    Função adicionada para compatibilidade com o seu main.py.

    Motivo:
    - O seu main.py faz: `from app.config import get_settings`
    - Sem isso, ocorre ImportError na inicialização.

    Regra:
    - Não muda fluxo do projeto: apenas devolve a instância já existente.
    """
    return settings
