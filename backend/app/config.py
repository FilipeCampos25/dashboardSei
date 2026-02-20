from __future__ import annotations

from pathlib import Path

from dotenv import find_dotenv, load_dotenv
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _load_env() -> None:
    """Load .env from cwd or parent folders."""
    env_path = find_dotenv(filename=".env", usecwd=True)
    if env_path:
        load_dotenv(env_path, override=False)
        return

    here = Path(__file__).resolve()
    for parent in [here.parent, *here.parents]:
        candidate = parent / ".env"
        if candidate.exists():
            load_dotenv(str(candidate), override=False)
            return


_load_env()


class Settings(BaseSettings):
    """Project runtime settings loaded from environment variables."""

    model_config = SettingsConfigDict(extra="ignore")

    sei_url: str | None = Field(
        default=None,
        validation_alias=AliasChoices("url_sei", "URL", "SEI_URL", "URL_SEI"),
    )
    username: str | None = Field(
        default=None,
        validation_alias=AliasChoices("username", "USERNAME", "USER", "SEI_USERNAME"),
    )
    password: str | None = Field(
        default=None,
        validation_alias=AliasChoices("password", "PASSWORD", "PASS", "SEI_PASSWORD"),
    )

    headless: bool = Field(
        default=False,
        validation_alias=AliasChoices("HEADLESS", "headless"),
    )
    timeout_seconds: int = Field(
        default=20,
        validation_alias=AliasChoices("TIMEOUT_SECONDS", "timeout_seconds"),
    )
    manual_login: bool = Field(
        default=True,
        validation_alias=AliasChoices("MANUAL_LOGIN", "manual_login"),
    )
    manual_login_wait_seconds: int = Field(
        default=120,
        validation_alias=AliasChoices(
            "MANUAL_LOGIN_WAIT_SECONDS",
            "manual_login_wait_seconds",
        ),
    )
    debug: bool = Field(
        default=False,
        validation_alias=AliasChoices("DEBUG", "debug"),
    )
    output_dir: str = Field(
        default="output",
        validation_alias=AliasChoices("OUTPUT_DIR", "output_dir"),
    )
    report_name: str = Field(
        default="report.json",
        validation_alias=AliasChoices("REPORT_NAME", "report_name"),
    )
    log_level: str = Field(
        default="INFO",
        validation_alias=AliasChoices("LOG_LEVEL", "log_level"),
    )
    descricoes_busca: str = Field(
        default="",
        validation_alias=AliasChoices("DESCRICOES_BUSCA", "descricoes_busca"),
    )
    descricoes_match_mode: str = Field(
        default="contains",
        validation_alias=AliasChoices("DESCRICOES_MATCH_MODE", "descricoes_match_mode"),
    )


settings = Settings()


def get_settings() -> Settings:
    return settings
