from __future__ import annotations

import logging

from app.config import get_settings
from app.core.logging_config import setup_logging
from app.rpa.scraping import SEIScraper


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = logging.getLogger(__name__)

    logger.info("Iniciando fluxo assistido no SEI")

    # Importante: para login manual/2FA, configure HEADLESS=False no .env.
    scraper = SEIScraper()
    try:
        scraper.run_full_flow(
            manual_login=True,
            # Limites iniciais para você validar se está no caminho certo:
            max_internos=3,
            max_processos_por_interno=5,
        )
        logger.info("Fluxo assistido finalizado")
    finally:
        scraper.driver.quit()


if __name__ == "__main__":
    main()
