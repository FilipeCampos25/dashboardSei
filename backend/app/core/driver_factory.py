"""
app/core/driver_factory.py
Cria um Chrome WebDriver de forma estavel em ambientes corporativos.

Por que esse arquivo existe:
- Muitas redes corporativas quebram downloads automaticos (SSL MITM), e o
  `webdriver_manager` tenta baixar o chromedriver em tempo de execucao, dando
  erro de certificado.
- O Selenium (4.6+) ja traz o Selenium Manager, que tenta resolver o driver
  automaticamente sem depender do webdriver_manager.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


def _prepare_managed_download_dir() -> Path:
    download_dir = Path(__file__).resolve().parents[2] / "output" / "browser_downloads"
    download_dir.mkdir(parents=True, exist_ok=True)
    for child in download_dir.iterdir():
        try:
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)
        except Exception:
            continue
    return download_dir


def _configure_download_prefs(options: Options) -> Path:
    download_dir = _prepare_managed_download_dir()
    options.add_experimental_option(
        "prefs",
        {
            "download.default_directory": str(download_dir),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "profile.default_content_setting_values.automatic_downloads": 1,
            "plugins.always_open_pdf_externally": True,
            "safebrowsing.enabled": True,
        },
    )
    return download_dir


def _finalize_driver_downloads(driver: webdriver.Chrome, download_dir: Path) -> webdriver.Chrome:
    try:
        driver.execute_cdp_cmd(
            "Page.setDownloadBehavior",
            {
                "behavior": "allow",
                "downloadPath": str(download_dir),
            },
        )
    except Exception:
        pass
    setattr(driver, "_sei_download_dir", str(download_dir))
    return driver


def create_chrome_driver(*, headless: bool = False) -> webdriver.Chrome:
    options = Options()

    if headless:
        options.add_argument("--headless=new")
    else:
        options.add_argument("--start-maximized")

    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    download_dir = _configure_download_prefs(options)

    chromedriver_path = os.getenv("CHROMEDRIVER_PATH")
    if chromedriver_path:
        service = Service(executable_path=chromedriver_path)
        driver = webdriver.Chrome(service=service, options=options)
        driver = _finalize_driver_downloads(driver, download_dir)
        if not headless:
            try:
                driver.maximize_window()
            except Exception:
                pass
        return driver

    driver = webdriver.Chrome(options=options)
    driver = _finalize_driver_downloads(driver, download_dir)
    if not headless:
        try:
            driver.maximize_window()
        except Exception:
            pass
    return driver
