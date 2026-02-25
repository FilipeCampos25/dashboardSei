from __future__ import annotations

import argparse
import logging
import sys

from app.config import get_settings
from app.core.logging_config import setup_logging
from app.rpa.scraping import SEIScraper


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SEI assisted scraper")
    parser.add_argument("--debug", action="store_true", help="Force DEBUG log level")
    parser.add_argument("--manual-login", action="store_true", help="Wait for manual login")
    parser.add_argument("--auto-login", action="store_true", help="Try automated login")
    parser.add_argument("--max-internos", type=int, default=0)
    parser.add_argument("--max-processos", type=int, default=0)
    if hasattr(argparse, "BooleanOptionalAction"):
        parser.add_argument(
            "--stop-at-filter",
            action=argparse.BooleanOptionalAction,
            default=True,
            help="Open process filter and stop there (default: true). Use --no-stop-at-filter for manual debug.",
        )
    else:
        parser.add_argument("--stop-at-filter", dest="stop_at_filter", action="store_true", default=True)
        parser.add_argument("--no-stop-at-filter", dest="stop_at_filter", action="store_false")
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

    max_internos = args.max_internos if args.max_internos > 0 else None
    max_processos = args.max_processos if args.max_processos > 0 else None

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
            max_internos=max_internos,
            max_processos_por_interno=max_processos,
            stop_at_filter=args.stop_at_filter,
        )
        logger.info("Fluxo assistido finalizado")
        if not args.stop_at_filter and sys.stdin and sys.stdin.isatty():
            try:
                input("Filtro aberto para debug manual. Pressione ENTER para encerrar o navegador...")
            except EOFError:
                logger.warning("STDIN indisponivel; encerrando navegador sem pausa adicional.")
    except KeyboardInterrupt:
        logger.warning("Execucao interrompida pelo usuario (Ctrl+C).")
    finally:
        try:
            scraper.driver.quit()
        except Exception as exc:
            logger.debug("Driver ja encerrado ou indisponivel no encerramento: %s", exc)


if __name__ == "__main__":
    main()
