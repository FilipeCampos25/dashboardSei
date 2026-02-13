from __future__ import annotations

from functools import lru_cache
from os import getenv
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    sei_url: str
    username: str
    password: str
    headless: bool
    timeout_seconds: int
    output_dir: str
    report_name: str
    log_level: str


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    load_dotenv()

    headless_env = getenv("HEADLESS", "true").strip().lower()

    return Settings(
        sei_url=getenv("url_sei", "https://sei.defesa.gov.br/").strip().strip("'\""),
        username=getenv("username", "").strip().strip("'\""),
        password=getenv("password", "").strip().strip("'\""),
        headless=headless_env in {"1", "true", "yes", "on"},
        timeout_seconds=int(getenv("TIMEOUT_SECONDS", "20")),
        output_dir=getenv("OUTPUT_DIR", "output").strip(),
        report_name=getenv("REPORT_NAME", "sei_dashboard").strip(),
        log_level=getenv("LOG_LEVEL", "INFO").strip().upper(),
    )
