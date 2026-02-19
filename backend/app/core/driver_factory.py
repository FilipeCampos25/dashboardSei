from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def create_chrome_driver(headless: bool = False):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--start-maximized")

    # Selenium Manager resolve o driver automaticamente (sem webdriver_manager)
    driver = webdriver.Chrome(options=options)
    return driver
