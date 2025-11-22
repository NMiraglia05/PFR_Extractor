from abc import ABC, abstractmethod
import re
import time
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

class HTML_Scraper(ABC):
    @abstractmethod
    def load_page(self):
        """Unique method for extracting raw html"""
        raise NotImplementedError()

    @abstractmethod
    def quit(self):
        """Whatever method is required for cleanup"""
        raise NotImplementedError()

class ExtractionFailed(Exception):
    pass

class Scrape_HTML:
    def __init__(self):
        self.test_request()

    def test_request(self):
        resp = requests.get('https://www.pro-football-reference.com/boxscores/202409080buf.htm')
        raw_html = resp.text

        test_html = re.sub(r'<!--.*?-->', '', raw_html, flags=re.DOTALL)
        soup = BeautifulSoup(test_html, 'html.parser')
        table = soup.find('table', id='passing_advanced')

        if table:
            self.access = scrape_with_requests()
        else:
            self.access=scrape_with_selenium() #requests can access the webpage just fine, but sometimes gets blocked by anti-bot filters. This is session-wide, not page specific. Selenium does not have this problem.

    def scrape(self, url,attempt=1,max_attempts=3):
        """load_page methods do not parse HTML into BeautifulSoup. Sometimes the HTML is immediately parsed, but in many cases it is stored for later processing—after Selenium has finished—to improve efficiency."""
        try:
            html=self.access.load_page(url)
        except ExtractionFailed:
            if attempt<max_attempts: # since 3 is not greater than 3, this will trigger a failure on loop 3
                logging.warning(f'Attempt {attempt} failed. Retrying...')
                attempt+=1
                time.sleep(6) # ensures the halt always happens, since running load_page will query the server again(only happens in case of failure, there is never a double sleep)
                return self.scrape(url,attempt)
            else:
                raise ExtractionFailed
        time.sleep(6) # ensures compliance with PFR rate limit
        return html
    
    def quit(self):
       self.access.quit()

class scrape_with_requests(HTML_Scraper):
    def __init__(self):
        pass

    def load_page(self, url):
        try:
            resp = requests.get(url)
            html = re.sub(r'<!--.*?-->', '', resp.text, flags=re.DOTALL)
            return html

        except Exception:
            logging.warning(f'FAILED requests scrape of: {url}')
            raise ExtractionFailed
        
    def quit(self):
        pass

class scrape_with_selenium(HTML_Scraper):
    def __init__(self):
        self.start_driver()

    def load_page(self, url):
        logging.debug(f'Attempting Selenium scrape for {url}')

        try:
            self.driver.get(url)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        except Exception as e:
            logging.debug(f'While attempting to extract page, the following error occurred: {e}')
            raise ExtractionFailed

        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
        except:
            logging.error("Timed out waiting for table in Selenium")
            raise ExtractionFailed

        return self.driver.page_source   

    def start_driver(self):
        logging.info('No active driver detected, starting new webdriver...')
        service = Service(ChromeDriverManager().install())
        options = Options()
        options.add_argument("--headless")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--enable-unsafe-swiftshader")
        options.add_argument("--log-level=3")
        options.add_argument("window-size=1920,1080")
        options.add_argument("--ignore-certificate-errors")
        self.driver = webdriver.Chrome(service=service, options=options)

    def quit(self):
        self.driver.quit()
        logging.info('Webdriver successfuly closed.\n')
