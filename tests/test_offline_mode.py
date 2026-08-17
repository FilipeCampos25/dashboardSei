from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "backend"))

from app.config import OfflineModeError, settings  # noqa: E402
from app.core.driver_factory import create_chrome_driver  # noqa: E402
from app.integrations.transferegov_client import consultar_ted  # noqa: E402
from app.rpa.scraping import SEIScraper  # noqa: E402
import main as backend_main  # noqa: E402


class OfflineModeTests(unittest.TestCase):
    @staticmethod
    def _offline_scraper() -> SEIScraper:
        scraper = SEIScraper.__new__(SEIScraper)
        scraper.settings = SimpleNamespace(offline_only=True)
        scraper.driver = Mock()
        return scraper

    def test_offline_constructor_fails_before_create_chrome_driver(self) -> None:
        offline_settings = SimpleNamespace(offline_only=True)
        with patch("app.rpa.scraping.get_settings", return_value=offline_settings), patch(
            "app.rpa.scraping.create_chrome_driver"
        ) as create_driver:
            with self.assertRaisesRegex(OfflineModeError, "OFFLINE_ONLY.*SEIScraper.__init__"):
                SEIScraper()

        create_driver.assert_not_called()

    def test_offline_main_fails_before_logging_or_scraper_instantiation(self) -> None:
        offline_settings = SimpleNamespace(offline_only=True)
        with patch.object(backend_main, "get_settings", return_value=offline_settings), patch.object(
            backend_main, "setup_logging"
        ) as setup_logging, patch.object(backend_main, "SEIScraper") as scraper_class, patch.object(
            sys, "argv", ["main.py"]
        ):
            with self.assertRaisesRegex(OfflineModeError, "OFFLINE_ONLY.*main"):
                backend_main.main()

        setup_logging.assert_not_called()
        scraper_class.assert_not_called()

    def test_direct_driver_creation_fails_before_selenium(self) -> None:
        with patch.object(settings, "offline_only", True), patch(
            "app.core.driver_factory.webdriver.Chrome"
        ) as selenium_chrome, patch(
            "app.core.driver_factory._prepare_managed_download_dir"
        ) as prepare_downloads:
            with self.assertRaisesRegex(OfflineModeError, "OFFLINE_ONLY.*create_chrome_driver"):
                create_chrome_driver()

        selenium_chrome.assert_not_called()
        prepare_downloads.assert_not_called()

    def test_offline_full_flow_and_logins_fail_before_driver_use(self) -> None:
        scraper = self._offline_scraper()

        for operation in (
            lambda: scraper.run_full_flow(),
            lambda: scraper._wait_for_manual_login(),
            lambda: scraper._login_if_possible(),
        ):
            with self.subTest(operation=operation):
                with self.assertRaisesRegex(OfflineModeError, "OFFLINE_ONLY"):
                    operation()

        scraper.driver.get.assert_not_called()

    def test_offline_navigation_guard_prevents_driver_get(self) -> None:
        scraper = self._offline_scraper()

        with self.assertRaisesRegex(OfflineModeError, "OFFLINE_ONLY"):
            scraper._restore_process_base_context("00000", process_url="https://invalid.test/sei")
        with self.assertRaisesRegex(OfflineModeError, "OFFLINE_ONLY"):
            scraper._click_selected_interno(Mock(), "target", "https://invalid.test/sei")

        scraper.driver.get.assert_not_called()

    def test_offline_http_guard_prevents_requests(self) -> None:
        with patch.object(settings, "offline_only", True), patch(
            "app.integrations.transferegov_client.requests.get"
        ) as requests_get:
            with self.assertRaisesRegex(OfflineModeError, "OFFLINE_ONLY.*consultar_ted"):
                consultar_ted("00000", "1", 2026)

        requests_get.assert_not_called()

    def test_online_constructor_keeps_legacy_interface_with_mocked_driver(self) -> None:
        driver = Mock()
        with patch.object(settings, "offline_only", False), patch(
            "app.rpa.scraping.create_chrome_driver", return_value=driver
        ) as create_driver, patch("app.rpa.scraping.WebDriverWait"):
            scraper = SEIScraper()

        create_driver.assert_called_once_with(headless=settings.headless)
        self.assertIs(scraper.driver, driver)


if __name__ == "__main__":
    unittest.main()
