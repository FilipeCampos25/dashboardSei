from __future__ import annotations

"""Configuration for the isolated TED/ACT module."""

from pathlib import Path
from urllib.parse import urljoin

from dotenv import find_dotenv, load_dotenv
from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _load_env() -> None:
    """Load the nearest .env file without modifying existing application config."""
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


class TedActModuleConfig(BaseSettings):
    """Runtime settings for the TED/ACT module."""

    model_config = SettingsConfigDict(extra="ignore", populate_by_name=True)

    api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("TED_ACT_API_KEY", "ted_act_api_key"),
    )
    ted_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices("TED_API_KEY", "ted_api_key"),
    )
    act_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "ACT_API_KEY",
            "PORTAL_TRANSPARENCIA_API_KEY",
            "act_api_key",
        ),
    )
    ted_base_url: str | None = Field(
        default="https://api.transferegov.gestao.gov.br/ted",
        validation_alias=AliasChoices(
            "TED_API_BASE_URL",
            "TED_ACT_API_BASE_URL",
            "ted_api_base_url",
        ),
    )
    act_base_url: str | None = Field(
        default="https://api.portaldatransparencia.gov.br/api-de-dados",
        validation_alias=AliasChoices(
            "ACT_API_BASE_URL",
            "TED_ACT_API_BASE_URL",
            "act_api_base_url",
        ),
    )
    ted_endpoint_path: str = Field(
        default="termo_execucao",
        validation_alias=AliasChoices("TED_API_ENDPOINT_PATH", "ted_api_endpoint_path"),
    )
    act_endpoint_path: str = Field(
        default="convenios",
        validation_alias=AliasChoices("ACT_API_ENDPOINT_PATH", "act_api_endpoint_path"),
    )
    ted_process_query_param: str = Field(
        default="tx_num_processo_sei",
        validation_alias=AliasChoices("TED_PROCESS_QUERY_PARAM", "ted_process_query_param"),
    )
    act_process_query_param: str = Field(
        default="numero",
        validation_alias=AliasChoices("ACT_PROCESS_QUERY_PARAM", "act_process_query_param"),
    )
    act_api_key_header: str = Field(
        default="chave-api-dados",
        validation_alias=AliasChoices("ACT_API_KEY_HEADER", "act_api_key_header"),
    )
    verify_ssl: bool = Field(
        default=True,
        validation_alias=AliasChoices("TED_ACT_VERIFY_SSL", "ted_act_verify_ssl"),
    )
    input_dir: str = Field(
        default="debug",
        validation_alias=AliasChoices("TED_ACT_INPUT_DIR", "ted_act_input_dir"),
    )
    output_dir: str = Field(
        default="output/ted_act",
        validation_alias=AliasChoices("TED_ACT_OUTPUT_DIR", "ted_act_output_dir"),
    )
    timeout_seconds: int = Field(
        default=20,
        validation_alias=AliasChoices("TED_ACT_TIMEOUT_SECONDS", "ted_act_timeout_seconds"),
    )

    @property
    def input_path(self) -> Path:
        """Return the configured snapshot input directory."""
        return Path(self.input_dir)

    @property
    def output_path(self) -> Path:
        """Return the configured output directory for TED/ACT artifacts."""
        return Path(self.output_dir)

    @property
    def ted_endpoint_url(self) -> str | None:
        """Return the full TED endpoint URL."""
        if not self.ted_base_url:
            return None
        return urljoin(self.ted_base_url.rstrip("/") + "/", self.ted_endpoint_path.lstrip("/"))

    @property
    def act_endpoint_url(self) -> str | None:
        """Return the full ACT endpoint URL."""
        if not self.act_base_url or not self.act_endpoint_path:
            return None
        return urljoin(self.act_base_url.rstrip("/") + "/", self.act_endpoint_path.lstrip("/"))

    @property
    def resolved_ted_api_key(self) -> str | None:
        """Return the TED-specific API key or the shared one."""
        return self.ted_api_key or self.api_key

    @property
    def resolved_act_api_key(self) -> str | None:
        """Return the ACT-specific API key or the shared one."""
        return self.act_api_key or self.api_key


def load_ted_act_config() -> TedActModuleConfig:
    """Build and return module settings from environment variables."""
    return TedActModuleConfig()
