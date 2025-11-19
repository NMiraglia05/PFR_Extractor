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
import hashlib
import numpy as np

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
    logging.info(f'Extraction successful\n\n')
    time.sleep(6) # ensures compliance with PFR's max of 10 requests per minute
    return driver.page_source   

class DIM_Players_Mixin:
    def generate_player_id(self, name_col, birth_col):
        self.df['normalized_name'] = self.normalize_names_column(name_col)
        self.df['Name'] = self.df['Player']
        self.df['Player'] = self.generate_hash(self.df['normalized_name'], birth_col)
        self.df['Player_ID'] = self.df['Player'] + f'_{self.year}'
        self.df.drop(columns=[c for c in ['normalized_name', 'Birthdate_str'] if c in self.df.columns], inplace=True)
        cols=['Player_ID','Player','Name']+[c for c in self.df.columns if c not in ['Player_ID','Player','Name']]
        self.df=self.df[cols]
        
    @staticmethod
    def normalize_names_column(col:pd.Series)->pd.Series:
        suffixes=['jr','sr','iii','ii','iv']
        col_clean=col.str.replace('-',' ',regex=False)
        col_split=col_clean.str.split()
        col_filtered=col_split.apply(lambda parts:[p for p in parts if p.lower().rstrip('.') not in suffixes])
        return col_filtered.apply(lambda parts:parts[0]+parts[-1] if len(parts)>=2 else parts[0])

    @staticmethod
    def generate_hash(name_col,birth_col):
        combined=(name_col.str.lower()+birth_col.str.replace('/','',regex=False)).values.astype('U')
        def vectorized_sha256(arr):
            return np.array([hashlib.sha256(s.encode('utf-8')).hexdigest()[:8] for s in arr])
        return pd.Series(vectorized_sha256(combined),index=name_col.index)

service = Service(ChromeDriverManager().install())

class Fact(Table): 
    def calculate_values(self):
        self.clean_and_convert(self.category)

        for calc in self.category.calc_columns:
            dicref=self.category.calc_columns[calc]
            for col in dicref:
                nestref=dicref[col]
                if calc=='avg':
                    self.df[col]=self.df[nestref[0]]/self.df[nestref[1]]
                if calc=='pct':
                    self.df[col]=(self.df[nestref[0]]*100)/self.df[nestref[1]]
                if calc=='tot':
                    self.df[col]=self.df[nestref[0]]*self.df[nestref[1]]
                if calc=='sum':
                    self.df[col]=self.df[nestref[0]]+self.df[nestref[1]]
        logging.info(f'After calculating, this is the table:\n\n{self.df}')

        self.df=self.df[self.category.col_order]

    def long_now(self):
        logging.info(f'Before lengthening, this is the dataframe:\n\n{self.df}')
        self.df=self.df.melt(id_vars=['Player','Tm'],value_vars=self.value_vars,var_name='Stat',value_name='Value')

        self.df['Stat']=self.df['Stat'].map(self.stat_lookup).fillna(0)

    def clean_and_convert(self,category):
        cleaning = getattr(category, 'cleaning', None)
        if cleaning:
            self.clean_table()
        self.df = self.df.astype(category.expected_cols)
