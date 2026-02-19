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

Mudança feita aqui (mínima):
- Corrige imports quebrados:
  - Removemos dependência de `app.services.config` (não existe no projeto).
  - Mantemos o fluxo igual, só apontando para `app.config.get_settings` (agora existe).
  - `app.services.selectors` também não existia, então criamos (em outro item abaixo)
    e mantemos o mesmo nome do import para não mexer no restante.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Set

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Mantido: settings pode ser útil para debug/compatibilidade (não alterei fluxo)
from app.config import settings

from app.core.driver_factory import create_chrome_driver
from app.core.logging_config import setup_logger

# AQUI está a correção do erro futuro:
# Antes estava: from app.services.config import get_settings  (arquivo não existe)
# Agora apontamos pro lugar correto (app.config), que também é o que o main.py usa.
from app.config import get_settings

# Mantido o import para não mexer no restante: nós vamos criar esse arquivo em app/services/selectors.py
from app.services.selectors import load_selectors


@dataclass
class FoundItem:
    """Representa um item encontrado na árvore/documentos."""
    text: str


class SEIScraper:
    def __init__(self) -> None:
        """
        Inicializa o scraper.

        IMPORTANTE (mantido):
        - O login pode ser manual (fluxo assistido). Então a automação não tenta
          burlar 2FA / token temporário: ela apenas espera você finalizar.
        - As variáveis principais vêm do app.config.settings (carregado do .env),
          mas também aceitamos chaves antigas por compatibilidade.

        Regras respeitadas:
        - Não alterei fluxo de scraping.
        - Não otimizei.
        - Apenas corrigi imports/config de apoio.
        """

        self.logger = setup_logger()

        # Não sombrear o `settings` do app.config
        cfg = get_settings()

        # Prefer Settings (do .env.example: url_sei/username/password)
        # e aceita também variantes antigas (URL/USERNAME/PASSWORD).
        self.base_url = (
            cfg.sei_url
            or os.getenv("URL")
            or os.getenv("url_sei")
            or os.getenv("SEI_URL")
            or os.getenv("URL_SEI")
        )
        self.username = (
            cfg.username
            or os.getenv("USERNAME")
            or os.getenv("username")
            or os.getenv("USER")
        )
        self.password = (
            cfg.password
            or os.getenv("PASSWORD")
            or os.getenv("password")
            or os.getenv("PASS")
        )

        # Driver/Wait (mantido)
        self.driver = create_chrome_driver(headless=cfg.headless)
        self.wait = WebDriverWait(self.driver, cfg.timeout_seconds)

        # XPaths e seletores (mantido)
        # - Agora `load_selectors()` existe (arquivo criado abaixo)
        self.selectors = load_selectors()

        # Coleta em memória (pra imprimir lista pronta no fim)
        self.found: Set[str] = set()

    # =========================================================
    # Fluxo público
    # =========================================================
    def run_full_flow(
        self,
        manual_login: bool = True,
        max_internos: int = 3,
        max_processos_por_interno: int = 5,
    ) -> List[str]:
        """
        Executa o fluxo completo.

        - manual_login: se True, abre a página e espera você logar manualmente.
        - max_internos / max_processos_por_interno: limites para testes (mantido).

        Retorna:
        - Lista de textos únicos encontrados (ordenados no final).
        """
        if not self.base_url:
            # Mantido: erro claro se não tiver URL
            raise RuntimeError("Config ausente: sei_url / url_sei / URL / SEI_URL / URL_SEI")

        self.logger.info("Abrindo SEI em: %s", self.base_url)
        self.driver.get(self.base_url)

        if manual_login:
            # Mantido: modo assistido
            self.logger.info("Aguardando login manual... (faça login e navegue até a tela inicial)")
            input("Quando terminar o login manual, pressione ENTER para continuar...")
        else:
            # Mantido: se alguém quiser automatizar credenciais (não mexi)
            self._login_if_possible()

        # Mantido: tenta fechar popup e navega
        self._close_popup_if_exists()
        self._open_interno_menu()

        internos = self._list_internos()
        internos = internos[:max_internos]

        for interno_text in internos:
            self.logger.info("Acessando interno: %s", interno_text)
            self._open_interno(interno_text)

            processos = self._list_processos()
            processos = processos[:max_processos_por_interno]

            for proc in processos:
                self._open_processo(proc)
                self._collect_documentos()
                self._close_current_tab_and_back()

            self._back_to_interno_list()

        # Mantido: imprime lista pronta
        result = sorted(self.found)
        self.logger.info("Itens únicos encontrados: %d", len(result))
        print(result)
        return result

    # =========================================================
    # Fluxos internos (mantidos)
    # =========================================================
    def _login_if_possible(self) -> None:
        """
        Login automatizado (opcional).
        Mantido conforme intenção original: se você quiser usar username/password,
        pode plugar aqui. Não alterei lógica, só preservo o método.
        """
        # Se não tiver credenciais, não força nada
        if not self.username or not self.password:
            self.logger.info("Sem credenciais no env; pulando login automatizado.")
            return

        # Seletores do JSON
        login_sel = self.selectors.get("login", {})
        x_user = login_sel.get("username")
        x_pass = login_sel.get("password")
        x_btn = login_sel.get("acessar")

        if not (x_user and x_pass and x_btn):
            self.logger.info("Seletores de login não encontrados; pulando login automatizado.")
            return

        try:
            self.wait.until(EC.presence_of_element_located((By.XPATH, x_user))).send_keys(self.username)
            self.wait.until(EC.presence_of_element_located((By.XPATH, x_pass))).send_keys(self.password)
            self.wait.until(EC.element_to_be_clickable((By.XPATH, x_btn))).click()
            self.logger.info("Login automatizado enviado.")
        except Exception as e:
            self.logger.exception("Falha no login automatizado (mantido): %s", e)

    def _close_popup_if_exists(self) -> None:
        """Fecha pop-up se existir (mantido)."""
        sel = self.selectors.get("tela_inicio", {})
        x = sel.get("remove_pup_pop")
        if not x:
            return
        try:
            # espera pequena e tenta clicar
            time.sleep(1)
            self.driver.find_element(By.XPATH, x).click()
            self.logger.info("Pop-up fechado.")
        except Exception:
            # Mantido: se não existir, segue
            return

    def _open_interno_menu(self) -> None:
        """Abre menu Bloco -> Interno (mantido)."""
        sel = self.selectors.get("tela_inicio", {})
        x_bloco = sel.get("bloco")
        x_interno = sel.get("interno")

        if not (x_bloco and x_interno):
            raise RuntimeError("Seletores de menu (bloco/interno) ausentes em xpath_selector.json")

        self.wait.until(EC.element_to_be_clickable((By.XPATH, x_bloco))).click()
        self.wait.until(EC.element_to_be_clickable((By.XPATH, x_interno))).click()

    def _list_internos(self) -> List[str]:
        """Lista números internos (mantido)."""
        sel = self.selectors.get("interno", {})
        x = sel.get("numero_interno")
        if not x:
            raise RuntimeError("Seletor 'interno.numero_interno' ausente em xpath_selector.json")

        elems = self.wait.until(EC.presence_of_all_elements_located((By.XPATH, x)))
        return [e.text.strip() for e in elems if e.text and e.text.strip()]

    def _open_interno(self, interno_text: str) -> None:
        """Abre um interno pelo texto (mantido)."""
        sel = self.selectors.get("interno", {})
        x = sel.get("numero_interno")
        if not x:
            raise RuntimeError("Seletor 'interno.numero_interno' ausente em xpath_selector.json")

        elems = self.driver.find_elements(By.XPATH, x)
        for e in elems:
            if e.text.strip() == interno_text:
                e.click()
                return

        raise RuntimeError(f"Não consegui abrir o interno: {interno_text}")

    def _list_processos(self) -> List[str]:
        """Lista processos dentro do interno (mantido)."""
        sel = self.selectors.get("interno", {})
        x = sel.get("processo")
        if not x:
            raise RuntimeError("Seletor 'interno.processo' ausente em xpath_selector.json")

        elems = self.wait.until(EC.presence_of_all_elements_located((By.XPATH, x)))
        # Aqui preservamos a ideia: usar o atributo/texto que o HTML fornecer
        out: List[str] = []
        for e in elems:
            t = e.text.strip() if e.text else ""
            if t:
                out.append(t)
        return out

    def _open_processo(self, processo_text: str) -> None:
        """Abre processo em nova aba (mantido)."""
        sel = self.selectors.get("interno", {})
        x = sel.get("processo")
        if not x:
            raise RuntimeError("Seletor 'interno.processo' ausente em xpath_selector.json")

        elems = self.driver.find_elements(By.XPATH, x)
        for e in elems:
            if (e.text or "").strip() == processo_text:
                e.click()
                time.sleep(1)
                # troca para última aba
                self.driver.switch_to.window(self.driver.window_handles[-1])
                return

        raise RuntimeError(f"Não consegui abrir o processo: {processo_text}")

    def _collect_documentos(self) -> None:
        """Coleta documentos no processo (mantido)."""
        sel = self.selectors.get("interno", {})
        x_docs = sel.get("documentos_do_processo")

        if not x_docs:
            self.logger.info("Seletor de documentos não encontrado; pulando coleta.")
            return

        try:
            elems = self.wait.until(EC.presence_of_all_elements_located((By.XPATH, x_docs)))
            for e in elems:
                t = (e.text or "").strip()
                if t:
                    if t not in self.found:
                        self.logger.info("[ACHEI] %s", t)
                    self.found.add(t)
        except Exception as e:
            self.logger.exception("Falha ao coletar documentos (mantido): %s", e)

    def _close_current_tab_and_back(self) -> None:
        """Fecha a aba atual do processo e volta para a anterior (mantido)."""
        try:
            if len(self.driver.window_handles) > 1:
                self.driver.close()
                self.driver.switch_to.window(self.driver.window_handles[-1])
        except Exception as e:
            self.logger.exception("Falha ao fechar aba/voltar (mantido): %s", e)

    def _back_to_interno_list(self) -> None:
        """Volta para lista de internos (mantido, simples)."""
        # Mantido: estratégia conservadora (não inventei navegação nova)
        try:
            self.driver.back()
            time.sleep(1)
        except Exception:
            return
