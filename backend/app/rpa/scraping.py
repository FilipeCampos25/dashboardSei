"""
Scraping assistido do SEI (via Selenium).

Objetivo imediato (escopo fechado):
- Login manual (para suportar validação extra/2FA/código temporário).
- Depois do login, o Selenium assume e percorre:
  1) Fecha pop-up (se existir)
  2) Abre menu: Bloco -> Interno
  3) Itera números internos
  4) Para cada interno, itera processos
  5) Cada processo abre em nova aba: entra, expande "plus" se existir, lista documentos
  6) Fecha a aba do processo e volta

Saída:
- Logs "[ACHEI] <texto_do_documento>" conforme encontrar documentos.
- Ao final, imprime uma LISTA Python com textos únicos encontrados,
  pronta para você copiar/colar e usar como base para filtros/regras.

Observações importantes:
- Para login manual, rode com HEADLESS=False no .env (ou variável de ambiente).
- Este arquivo usa os XPaths do backend/app/rpa/xpath_selector.json.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import List, Optional, Set, Tuple

from selenium.common.exceptions import StaleElementReferenceException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from app.config import get_settings
from app.core.driver_factory import create_chrome_driver


class SEIScraper:
    """Scraper assistido do SEI.

    Este class foi construído para ser executado via CLI (backend/main.py).
    """

    def __init__(self) -> None:
        # Config (via .env)
        self.base_url = os.getenv("URL")
        self.username = os.getenv("USERNAME")
        self.password = os.getenv("PASSWORD")

        # Driver (Chrome)
        # Importante: para login manual, HEADLESS precisa estar False.
        settings = get_settings()
        self.driver = create_chrome_driver(headless=settings.headless)

        # XPaths
        self.xpaths = self._load_xpaths()

    # ------------------------------------------------------------------
    # Entrada principal
    # ------------------------------------------------------------------

    def run_full_flow(
        self,
        *,
        manual_login: bool = True,
        max_internos: Optional[int] = None,
        max_processos_por_interno: Optional[int] = None,
        sleep_apos_login_s: float = 1.0,
        sleep_apos_menu_s: float = 1.5,
    ) -> List[str]:
        """Executa o fluxo completo e devolve uma lista única de documentos.

        Args:
            manual_login: se True, você faz o login/2FA manualmente e aperta ENTER.
            max_internos: limita quantos "números internos" varrer (debug/segurança).
            max_processos_por_interno: limita quantos processos por interno varrer.
            sleep_apos_login_s: delay curto (segundos) após login detectado.
            sleep_apos_menu_s: delay curto após clicar em Bloco -> Interno.

        Returns:
            Lista única (ordenada) com textos de documentos encontrados.
        """
        self._open_base_url()
        self._wait_for_login(manual=manual_login)
        time.sleep(max(0.0, sleep_apos_login_s))

        self._try_close_popup()
        self._open_bloco_interno(sleep_after=sleep_apos_menu_s)

        encontrados: Set[str] = set()

        internos = self._safe_find_all(By.XPATH, self.xpaths["interno"]["numero_interno"])
        print(f"[INFO] Números internos encontrados: {len(internos)}")

        for idx_interno in range(len(internos)):
            if max_internos is not None and idx_interno >= max_internos:
                print(f"[INFO] Limite max_internos={max_internos} atingido. Parando.")
                break

            # Rebusca a lista a cada iteração para evitar 'stale element'.
            internos = self._safe_find_all(By.XPATH, self.xpaths["interno"]["numero_interno"])
            if idx_interno >= len(internos):
                break

            interno_el = internos[idx_interno]
            interno_txt = self._safe_text(interno_el)
            print(f"\n[INFO] Abrindo interno {idx_interno+1}/{len(internos)}: {interno_txt!r}")
            self._safe_click(interno_el)

            # Alguns ambientes atualizam a árvore e a lista de processos demora.
            self._wait_any(
                [
                    (By.XPATH, self.xpaths["interno"]["processo"]),
                    (By.XPATH, self.xpaths["interno"]["documentos_do_processo"]),
                ],
                timeout=20,
            )

            processos = self._safe_find_all(By.XPATH, self.xpaths["interno"]["processo"])
            print(f"[INFO] Processos encontrados no interno: {len(processos)}")

            for idx_proc in range(len(processos)):
                if max_processos_por_interno is not None and idx_proc >= max_processos_por_interno:
                    print(
                        f"[INFO] Limite max_processos_por_interno={max_processos_por_interno} atingido."
                    )
                    break

                processos = self._safe_find_all(By.XPATH, self.xpaths["interno"]["processo"])
                if idx_proc >= len(processos):
                    break

                proc_el = processos[idx_proc]
                proc_txt = self._safe_text(proc_el)
                print(f"\n[INFO] Abrindo processo {idx_proc+1}/{len(processos)}: {proc_txt!r}")

                doc_texts = self._open_process_in_new_tab_and_collect_documents(proc_el)
                for t in doc_texts:
                    t = (t or "").strip()
                    if not t:
                        continue

                    if t not in encontrados:
                        print(f"[ACHEI] {t}")
                    encontrados.add(t)

        # Lista final (pronta para copiar/colar)
        lista_final = sorted(encontrados, key=lambda s: s.casefold())
        print("\n[RESULTADO] Lista única de documentos encontrados (copie/cole):")
        print(lista_final)
        return lista_final

    # ------------------------------------------------------------------
    # XPaths
    # ------------------------------------------------------------------

    def _load_xpaths(self) -> dict:
        """Carrega o xpath_selector.json."""
        file_path = Path(__file__).with_name("xpath_selector.json")
        with file_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    # ------------------------------------------------------------------
    # Passo 1: Login assistido
    # ------------------------------------------------------------------

    def _open_base_url(self) -> None:
        if not self.base_url:
            raise RuntimeError("Variável URL não definida (.env).")
        print("[INFO] Abrindo URL base...")
        self.driver.get(self.base_url)

    def _wait_for_login(self, *, manual: bool, timeout_s: int = 300) -> None:
        """Espera o usuário estar logado.

        Critério: o botão/menu "Bloco" (tela_inicio.bloco) fica presente na DOM.
        """
        if manual:
            print(
                "\n[ACAO] Faça o login MANUALMENTE na janela do navegador (incluindo 2FA, se aparecer).\n"
                "      Quando a tela inicial carregar, volte aqui e pressione ENTER.\n"
            )
            try:
                input("[AGUARDANDO] Pressione ENTER após concluir o login... ")
            except EOFError:
                # Se rodar num ambiente sem stdin, seguimos apenas pelo WebDriverWait.
                pass
        else:
            # Modo automático (se um dia você quiser): preenche user/pass e clica.
            # Mantive aqui pronto, mas não é o foco do seu cenário atual.
            self._safe_send_keys(By.XPATH, self.xpaths["login"]["username"], self.username or "")
            self._safe_send_keys(By.XPATH, self.xpaths["login"]["password"], self.password or "")
            self._safe_click_xpath(By.XPATH, self.xpaths["login"]["acessar"])

        WebDriverWait(self.driver, timeout_s).until(
            EC.presence_of_element_located((By.XPATH, self.xpaths["tela_inicio"]["bloco"]))
        )
        print("[INFO] Login detectado.")

    def _try_close_popup(self) -> None:
        """Fecha o pop-up de aviso se existir."""
        xp = self.xpaths["tela_inicio"].get("remove_pup_pop")
        if not xp:
            return
        try:
            el = WebDriverWait(self.driver, 3).until(EC.element_to_be_clickable((By.XPATH, xp)))
            el.click()
            print("[INFO] Pop-up fechado.")
        except TimeoutException:
            print("[INFO] Pop-up não apareceu (ok).")
        except Exception as e:
            print(f"[WARN] Não consegui fechar pop-up: {e}")

    # ------------------------------------------------------------------
    # Passo 2: Menu Bloco -> Interno
    # ------------------------------------------------------------------

    def _open_bloco_interno(self, *, sleep_after: float = 1.5) -> None:
        print("[INFO] Abrindo menu: Bloco -> Interno...")
        bloco = WebDriverWait(self.driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, self.xpaths["tela_inicio"]["bloco"]))
        )
        bloco.click()

        interno = WebDriverWait(self.driver, 15).until(
            EC.element_to_be_clickable((By.XPATH, self.xpaths["tela_inicio"]["interno"]))
        )
        interno.click()
        time.sleep(max(0.0, sleep_after))

    # ------------------------------------------------------------------
    # Passo 3: Processo em nova aba -> documentos
    # ------------------------------------------------------------------

    def _open_process_in_new_tab_and_collect_documents(self, proc_el: WebElement) -> List[str]:
        """Clica num processo (abre nova aba), coleta documentos e fecha a aba.

        Retorna lista de textos (na ordem que apareceram).
        """
        driver = self.driver
        original_handle = driver.current_window_handle
        before_handles = set(driver.window_handles)

        self._safe_click(proc_el)

        # Espera surgir nova aba
        try:
            WebDriverWait(driver, 15).until(lambda d: len(d.window_handles) > len(before_handles))
        except TimeoutException:
            print("[WARN] Processo não abriu nova aba (ou demorou demais). Tentando seguir na mesma aba.")

        after_handles = driver.window_handles
        new_handles = [h for h in after_handles if h not in before_handles]
        target_handle = new_handles[0] if new_handles else original_handle

        # Entra na aba do processo
        if target_handle != original_handle:
            driver.switch_to.window(target_handle)

        try:
            self._try_expand_plus()
            return self._collect_document_texts()
        finally:
            # Se foi nova aba, fecha e volta pra aba original.
            if target_handle != original_handle:
                try:
                    driver.close()
                except Exception:
                    pass
                driver.switch_to.window(original_handle)

    def _try_expand_plus(self) -> None:
        """Clica no ícone de "plus/pasta" se existir.

        No seu JSON, o XPath do plus veio sem "//" no começo.
        Aqui tentamos algumas variações comuns.
        """
        raw = self.xpaths["interno"].get("plus")
        if not raw:
            return

        candidates = []
        if raw.startswith("//") or raw.startswith("/"):
            candidates.append(raw)
        else:
            candidates.append("//" + raw)
            candidates.append(".//" + raw)

        for xp in candidates:
            try:
                el = WebDriverWait(self.driver, 2).until(EC.element_to_be_clickable((By.XPATH, xp)))
                el.click()
                print("[INFO] Plus/pasta expandido.")
                return
            except TimeoutException:
                continue
            except Exception as e:
                print(f"[WARN] Erro ao tentar expandir plus ({xp}): {e}")
                return

        print("[INFO] Plus/pasta não encontrado (ok).")

    def _collect_document_texts(self) -> List[str]:
        """Coleta textos dos documentos do processo."""
        xp = self.xpaths["interno"]["documentos_do_processo"]
        try:
            WebDriverWait(self.driver, 15).until(EC.presence_of_all_elements_located((By.XPATH, xp)))
        except TimeoutException:
            print("[WARN] Não encontrei documentos nesse processo (timeout).")
            return []

        docs = self._safe_find_all(By.XPATH, xp)
        textos: List[str] = []
        for d in docs:
            t = self._safe_text(d)
            if t:
                textos.append(t)

        print(f"[INFO] Documentos coletados: {len(textos)}")
        return textos

    # ------------------------------------------------------------------
    # Utils Selenium (robustez)
    # ------------------------------------------------------------------

    def _safe_find_all(self, by, value) -> List[WebElement]:
        try:
            return self.driver.find_elements(by, value)
        except Exception:
            return []

    def _safe_text(self, el: WebElement) -> str:
        try:
            return (el.text or "").strip()
        except StaleElementReferenceException:
            return ""
        except Exception:
            return ""

    def _safe_click(self, el: WebElement) -> None:
        # Scroll ajuda quando a árvore está fora da área visível.
        try:
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", el)
        except Exception:
            pass

        try:
            el.click()
        except Exception:
            # Fallback JS
            try:
                self.driver.execute_script("arguments[0].click();", el)
            except Exception as e:
                print(f"[WARN] Falha ao clicar no elemento: {e}")

    def _safe_click_xpath(self, by, value) -> None:
        els = self._safe_find_all(by, value)
        if not els:
            raise TimeoutException(f"Elemento não encontrado para clique: {value}")
        self._safe_click(els[0])

    def _safe_send_keys(self, by, value, text: str) -> None:
        el = WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((by, value)))
        el.clear()
        el.send_keys(text)

    def _wait_any(self, locators: List[Tuple[str, str]], *, timeout: int = 15) -> None:
        """Espera qualquer um dos locators aparecer (para navegar sem travar)."""

        def _any_present(_driver) -> bool:
            for by, value in locators:
                try:
                    if _driver.find_elements(by, value):
                        return True
                except Exception:
                    continue
            return False

        try:
            WebDriverWait(self.driver, timeout).until(_any_present)
        except TimeoutException:
            pass
