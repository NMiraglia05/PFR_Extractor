import pandas as pd
import hashlib
import numpy as np
from bs4 import BeautifulSoup
from abc import abstractmethod, ABC

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
    def summerge(self,merged_df): #pass a pre-merged df into this. Does not have merging logic since it's very context-dependent
            calc_cols=convert_col_names(merged_df)
            for col in calc_cols:
                merged_df[col]=merged_df[f'{col}_x']+merged_df[f'{col}_y']
                merged_df.drop(columns=[f'{col}_x',f'{col}_y'],inplace=True)
            return merged_df 

    def convert_col_names(self,df):
        cols=df.columns
        cols=[col for col in cols if '_x' in col or '_y' in col]
        col_count=len(cols)
        if col_count%2==1:
            raise TypeError('An unmatched value column was passed into summerge.')
        if col_count==0:
            logging.warning('No value columns passed into summerge.')
        clean_cols=[]
        for col in cols:
            col=col.replace('_x','')
            clean_cols.append(col)
        return clean_cols
    
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
