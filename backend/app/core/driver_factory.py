"""
app/core/driver_factory.py
Cria um Chrome WebDriver de forma estável em ambientes corporativos.

Por que esse arquivo existe:
- Muitas redes corporativas quebram downloads automáticos (SSL MITM), e o
  `webdriver_manager` tenta baixar o chromedriver em tempo de execução, dando
  erro de certificado.
- O Selenium (4.6+) já traz o Selenium Manager, que tenta resolver o driver
  automaticamente sem depender do webdriver_manager.

O que fazemos aqui:
- Usamos Selenium Manager por padrão (sem webdriver_manager).
- Se você estiver numa rede que bloqueia isso, você pode definir um caminho
  manual para o chromedriver:
    CHROMEDRIVER_PATH=C:\\caminho\\para\\chromedriver.exe
"""

from __future__ import annotations

import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service


def create_chrome_driver(*, headless: bool = False) -> webdriver.Chrome:
    """Cria um webdriver do Chrome.

    Args:
        headless: executa sem abrir janela.

    Returns:
        webdriver.Chrome
    """

    options = Options()

    # Headless "novo" (Chrome moderno). Mantém compatibilidade com headless antigo.
    if headless:
        options.add_argument("--headless=new")

    # Flags úteis para estabilidade
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    # Se você quiser apontar o chromedriver manualmente (pra fugir de bloqueio de rede):
    chromedriver_path = os.getenv("CHROMEDRIVER_PATH")
    if chromedriver_path:
        service = Service(executable_path=chromedriver_path)
        return webdriver.Chrome(service=service, options=options)

    # Caso contrário, Selenium Manager tenta resolver automaticamente.
    return webdriver.Chrome(options=options)
