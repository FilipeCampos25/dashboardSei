from __future__ import annotations

import logging

from app.config import get_settings
from app.rpa.core.driver_factory import create_chrome_driver
from app.rpa.core.logging_config import setup_logging
from app.rpa.scraping import SEIScraper


def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    logger = logging.getLogger(__name__)

    logger.info("Iniciando fluxo de acesso e login no SEI")
    driver = create_chrome_driver(headless=settings.headless)

    try:
        scraper = SEIScraper(driver=driver, settings=settings)
        scraper.run_login_only()
        logger.info("Fluxo de login finalizado com sucesso")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
