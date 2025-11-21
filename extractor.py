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
import requests
from bs4 import BeautifulSoup
import re
from abc import abstractmethod, ABC

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

class scraper_methods:
    def uncomment_html(self):
        return re.sub(r'<!--.*?-->', '', self.html, flags=re.DOTALL)

class HTML_Scraper(scraper_methods):
    def __init__(self):
        self.test_request()

    def test_request(self):
        resp = requests.get('https://www.pro-football-reference.com/boxscores/202409080buf.htm')
        raw_html = resp.text

        test_html = re.sub(r'<!--.*?-->', '', raw_html, flags=re.DOTALL)
        soup = BeautifulSoup(test_html, 'html.parser')
        table = soup.find('table', id='passing_advanced')

        if table:
            self.scraping = scrape_with_requests(self.uncomment_html)
        else:
            self.scraping = scrape_with_selenium(self.uncomment_html)

    def scrape(self, url):
        return self.scraping.load_page(url)
    
    def quit(self):
        if self.driver:
            driver.quit()
            logging.info('Webdriver successfully closed.')

class scrape_with_requests:
    def __init__(self, uncomment_callback):
        self.uncomment = uncomment_callback

    def load_page(self, url, attempt=1, max_attempts=3):
        try:
            resp = requests.get(url)
            html = re.sub(r'<!--.*?-->', '', resp.text, flags=re.DOTALL)
            return html

        except Exception:
            if attempt <= max_attempts:
                logging.warning(f'Requests attempt {attempt} failed, retrying...')
                return self.load_page(url, attempt+1)
            logging.error(f'FAILED requests scrape of: {url}')
            raise ExtractionFailed

class scrape_with_selenium:
    def __init__(self, uncomment_callback):
        self.uncomment = uncomment_callback
        self.start_driver()

    def load_page(self, url, attempt=1, max_attempts=3):
        logging.info(f'Attempting Selenium scrape for {url}')

        try:
            self.driver.get(url)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        except Exception:
            if attempt <= max_attempts:
                logging.warning(f'Selenium attempt {attempt} failed, retrying...')
                return self.load_page(url, attempt+1)
            logging.error(f'FAILED selenium scrape of: {url}')
            raise ExtractionFailed

        try:
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "table"))
            )
        except:
            logging.error("Timed out waiting for table in Selenium")
            raise

        time.sleep(6)  # PFR rate limit compliance
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

class MissingCols(Exception):
    pass

class Table: # move this to the extractor module
    def __init__(self,category,soup,validate=True):
        logging.debug(f'\nCreating dataframe for {category.cat}')
        for k,v in category.__dict__.items():
            if not k.startswith('__'):
                setattr(self,k,v)
        self.df=ExtractTable(soup,self.id).fillna(0).replace('',0)
        if validate==True:
            self.shapecheck()

    def shapecheck(self):
        logging.debug('Conducting shapecheck.')
        actual_cols=set(self.df.columns)
        expected=set(self.expected_cols.keys())
        self.missing_cols=expected-actual_cols
        if self.missing_cols:
            logging.critical(f'Shapecheck failed. The table is missing the following columns: {self.missing_cols}.')
            raise MissingCols
        
        leftover_cols=actual_cols-expected
        
        if leftover_cols:
            logging.warning(f'Shapecheck succeeded, however there are more columns than expected. Unexpected columns: {leftover_cols}. These will be retained.')

    def typecheck(self):
        for col in self.df.columns:
            expectedtype=self.expected_cols[col]
            actualtype=self.df[col].dtypes
            if expectedtype==actualtype:
                logging.debug('Typecheck succeeded')
                continue
            else:
                logging.debug(f'{col} failed typecheck. Expected type: {expectedtype}. Actual type: {actualtype}. Attempting conversion...')
                try:
                    self.df[col]=self.df[col].astype(expectedtype)
                    logging.debug('Successfully converted to expected type.')
                except Exception as e:
                    logging.error(f'Unable to convert{col}- {e}')

    def clean_table(self):
        for col, rules in self.cleaning.items():
            for rule in rules:
                dirtychar = rule['target']
                replacement = rule['replace_with']
                self.df[col] = self.df[col].str.replace(dirtychar, replacement, regex=False)


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

class Dim_Check(ABC):
    @property
    @abstractmethod
    def primary_key(self):
        """HTML class identifier"""
        raise NotImplementedError()

class Dimension(Table,Dim_Check):
    def validate_export(self):
        df=self.df
        dup_mask=df[self.primary_key].duplicated(keep=False)

        if not dup_mask.any():
            logging.debug('No duplicates found.')
            return

        self.dup_df=df[dup_mask].sort_values(self.primary_key)
        raise TypeError

class Exporter:
    def __init__(self):
        export_df_objects=[]
        validated_dfs=[]

        for obj in export_df_objects:
            obj.validate_export()
            validated_dfs.append(obj.df)

def start_html_scraper(url):
    html=requests.get(url)
