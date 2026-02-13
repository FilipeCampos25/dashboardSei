from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from selenium.common.exceptions import TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from app.config import Settings

logger = logging.getLogger(__name__)

XPATHS_FILE = Path(__file__).with_name("xpath_selector.json")
with XPATHS_FILE.open("r", encoding="utf-8") as file:
    XPATHS: dict[str, Any] = json.load(file)


class SEIScraper:
    def __init__(self, driver: WebDriver, settings: Settings):
        self.driver = driver
        self.settings = settings
        self.wait = WebDriverWait(driver, settings.timeout_seconds)

    def run(self) -> list[dict[str, Any]]:
        self.login()
        self.dismiss_popup_if_present()
        self.navigate_to_internal_block()
        return self.collect_data()

    def run_login_only(self) -> None:
        self.login()
        self.dismiss_popup_if_present()

    def login(self) -> None:
        self.driver.get(self.settings.sei_url)

        login = XPATHS["login"]
        self.wait.until(EC.visibility_of_element_located((By.XPATH, login["username"]))).send_keys(
            self.settings.username
        )
        self.driver.find_element(By.XPATH, login["password"]).send_keys(self.settings.password)
        self.driver.find_element(By.XPATH, login["acessar"]).click()
        logger.info("Login enviado para o SEI")

    def dismiss_popup_if_present(self) -> None:
        popup_xpath = XPATHS.get("tela_inicio", {}).get("remove_pup_pop")
        if not popup_xpath:
            return

        try:
            popup_button = WebDriverWait(self.driver, 3).until(
                EC.element_to_be_clickable((By.XPATH, popup_xpath))
            )
            popup_button.click()
            logger.info("Popup inicial fechado")
        except TimeoutException:
            logger.info("Popup inicial nao apareceu")

    def navigate_to_internal_block(self) -> None:
        start = XPATHS.get("tela_inicio", {})

        bloco_xpath = start.get("bloco")
        interno_xpath = start.get("interno")

        if bloco_xpath:
            self.wait.until(EC.element_to_be_clickable((By.XPATH, bloco_xpath))).click()
        if interno_xpath:
            self.wait.until(EC.element_to_be_clickable((By.XPATH, interno_xpath))).click()

        logger.info("Navegacao para bloco interno concluida")

    def collect_data(self) -> list[dict[str, Any]]:
        collected_at = datetime.now().isoformat(timespec="seconds")

        rows_xpath = XPATHS.get("coleta", {}).get("linhas_tabela")
        if rows_xpath:
            rows = self.driver.find_elements(By.XPATH, rows_xpath)
            records = [
                {
                    "collected_at": collected_at,
                    "status": "capturado",
                    "linha": row.text.strip(),
                    "url": self.driver.current_url,
                    "titulo": self.driver.title,
                }
                for row in rows
                if row.text.strip()
            ]
            if records:
                logger.info("%s linha(s) capturada(s)", len(records))
                return records

        logger.info("Nenhuma linha mapeada. Salvando metadados da sessao")
        return [
            {
                "collected_at": collected_at,
                "status": "sem_linhas_mapeadas",
                "linha": "",
                "url": self.driver.current_url,
                "titulo": self.driver.title,
            }
        ]
