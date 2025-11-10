import logging
from datetime import date 
import pandas as pd
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium import webdriver

logging.basicConfig(
    filename=f'logs/log_{date.today()}.txt',
    level=logging.INFO,
    format='%(levelname)s - %(message)s',
    filemode='w'
)
logging.info('Starting...\n')

class ExtractionFailed(Exception):
    pass

def ExtractRows(soup,id):
    james = soup.find('table', id=id)
    table = james.find('tbody')
    rows = table.find_all('tr')
    thead = james.find('thead')
    if thead:
        header_rows = thead.find_all('tr')
        if header_rows:
            headers = [th.get_text(strip=True) for th in header_rows[-1].find_all('th')]
    else:
        headers = None
    return rows, headers

def ExtractTable(soup,id):
    rows, headers = ExtractRows(soup,id)
    table_data = []
    for row in rows:
        cells = row.find_all(['td', 'th'])
        row_data = [cell.get_text(strip=True) for cell in cells]
        table_data.append(row_data)
    df = pd.DataFrame(table_data, columns=headers)
    return df

def load_page(url,attempt=1,max_attempts=3):
    global driver
    logging.info(f'attempting to extract html for {url}')

    if 'driver' not in globals() or driver is None:
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
            driver = webdriver.Chrome(service=service, options=options)
    try:
        driver.get(url)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    except Exception as e:
        if attempt<max_attempts:
            logging.warning(f'Attempt {attempt} failed- reattempting.')
            return load_page(url,attempt+1)
        else:
            logging.error(f'Unable to extract {url}- attempt 3 failed.')
            raise ExtractionFailed
    try:
        WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
    except:
        logging.error('Webdriver timed out while waiting for the element.')
        raise Exception
    time.sleep(6) # ensures compliance with PFR's max of 10 requests per minute
    logging.info(f'Successfuly extracted html for {url}')
    return driver.page_source   

service = Service(ChromeDriverManager().install())
