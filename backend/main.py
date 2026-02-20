from __future__ import annotations

import argparse
import logging

from app.config import get_settings
from app.core.logging_config import setup_logging
from app.rpa.scraping import SEIScraper


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SEI assisted scraper")
    parser.add_argument("--debug", action="store_true", help="Force DEBUG log level")
    parser.add_argument("--manual-login", action="store_true", help="Wait for manual login")
    parser.add_argument("--auto-login", action="store_true", help="Try automated login")
    parser.add_argument("--max-internos", type=int, default=3)
    parser.add_argument("--max-processos", type=int, default=5)
    return parser


def main() -> None:
    args = _build_parser().parse_args()
    settings = get_settings()

    log_level = "DEBUG" if (settings.debug or args.debug) else settings.log_level
    setup_logging(log_level)
    logger = logging.getLogger(__name__)

    if args.manual_login and args.auto_login:
        raise ValueError("Use apenas uma opcao: --manual-login ou --auto-login")

    manual_login = settings.manual_login
    if args.manual_login:
        manual_login = True
    if args.auto_login:
        manual_login = False

    logger.info("Iniciando fluxo assistido no SEI")
    logger.debug(
        "Debug ativo. headless=%s manual_login=%s timeout=%s",
        settings.headless,
        manual_login,
        settings.timeout_seconds,
    )

    scraper = SEIScraper()
    try:
        scraper.run_full_flow(
            manual_login=manual_login,
            max_internos=args.max_internos,
            max_processos_por_interno=args.max_processos,
        )
        logger.info("Fluxo assistido finalizado")
    except KeyboardInterrupt:
        logger.warning("Execucao interrompida pelo usuario (Ctrl+C).")
    finally:
        try:
            scraper.driver.quit()
        except Exception as exc:
            logger.debug("Driver ja encerrado ou indisponivel no encerramento: %s", exc)


if __name__ == "__main__":
    main()
