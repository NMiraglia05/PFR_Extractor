from abc import ABC, ABCMeta, abstractmethod
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import logging
from datetime import date
from extractor import ExtractTable, ExtractionFailed, DIM_Players_Mixin
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time

logging.basicConfig(
    filename=f'logs/log_{date.today()}.txt',
    level=logging.CRITICAL,
    format='%(levelname)s - %(message)s',
    filemode='w'
)

teams={
    'Buffalo Bills':{       #AFC East
        'url':'buf',
        'abbr':'BUF'
    },
    'Miami Dolphins':{
        'url':'mia',
        'abbr':'MIA'
    },
    'New England Patriots':{
        'url':'nwe',
        'abbr':'NWE'
    },
    'New York Jets':{
        'url':'nyj',
        'abbr':'NYJ'
    },
    'Baltimore Ravens':{    #AFC North
        'url':'rav',
        'abbr':'BAL'
    },
    'Cleveland Browns':{
        'url':'cle',
        'abbr':'CLE'
    },
    'Pittsburgh Steelers':{
        'url':'pit',
        'abbr':'PIT'
    },
    'Cincinnati Bengals':{
        'url':'cin',
        'abbr':'CIN'
    },
    'Kansas City Chiefs':{           #AFC West
        'url':'kan',
        'abbr':'KAN'
    },
    'Los Angeles Chargers':{
        'url':'sdg',
        'abbr':'LAC'
    },
    'Denver Broncos':{
        'url':'den',
        'abbr':'DEN'
    },
    'Las Vegas Raiders':{
        'url':'rai',
        'abbr':'LVR'
    },
    'Houston Texans':{      #AFC South
        'url':'htx',
        'abbr':'HOU'
    },
    'Indianapolis Colts':{
        'url':'clt',
        'abbr':'IND'
    },
    'Tennessee Titans':{
        'url':'oti',
        'abbr':'TEN'
    },
    'Jacksonville Jaguars':{
        'url':'jax',
        'abbr':'JAX'
    },
    'New York Giants':{        #NFC East
        'url':'nyg',
        'abbr':'NYG'
    },
    'Dallas Cowboys':{
        'url':'dal',
        'abbr':'DAL'
    },
    'Washington Commanders':{
        'url':'was',
        'abbr':'WAS'
    },
    'Philadelphia Eagles':{ 
        'url':'phi',
        'abbr':'PHI'
    },
    'Chicago Bears':{           #NFC North
        'url':'chi',
        'abbr':'CHI'
    },
    'Green Bay Packers':{
        'url':'gnb',
        'abbr':'GNB'
    },
    'Detroit Lions':{
        'url':'det',
        'abbr':'DET'
    },
    'Minnesota Vikings':{
        'url':'min',
        'abbr':'MIN'
    },
    'Los Angeles Rams':{                 #NFC West
        'url':'ram',
        'abbr':'LAR'
    },
    'San Francisco 49ers':{
        'url':'sfo',
        'abbr':'SFO'
    },
    'Seattle Seahawks':{
        'url':'sea',
        'abbr':'SEA'
    },
    'Arizona Cardinals':{
        'url':'crd',
        'abbr':'ARI'
    },
    'Tampa Bay Buccaneers':{    #NFC South
        'url':'tam',
        'abbr':'TAM'
    },
    'Atlanta Falcons':{
        'url':'atl',
        'abbr':'ATL'
    },
    'New Orleans Saints':{
        'url':'nor',
        'abbr':'NOR'
    },
    'Carolina Panthers':{
        'url':'car',
        'abbr':'CAR'
    }
}

summary_stats=['P1','P2','P3','P4','P6','P8','P13','P15','P17','P18','P19','P20','P21','P23']#,'C1','C2','C3','C4','C5','C6','C8',
               #'C11','C13','C15','R1','R2','R3','R4','R5','R7','R9','D1','D2','D3','D5','D6','D7','D8','D9','D10',
               #'D11','D12','D13','D14','D15','D16','D17','D19','D22','D24','D25','D26','D27','D28','D29','D30',
               #'D31']

# base classes

class Table:
    def __init__(self,category,soup):
        logging.info(f'\nCreating dataframe for {category.cat}')
        for k,v in category.__dict__.items():
            if not k.startswith('__'):
                setattr(self,k,v)
        self.df=ExtractTable(soup,self.id).fillna(0).replace('',0)

        logging.debug('Shapecheck in progress...')
        if self.cat!='defense':
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

    def shapecheck(self):
        logging.debug('Conducting shapecheck.')
        actual_cols=set(self.df.columns)
        logging.info(self.df)
        expected=set(self.expected_cols.keys())
        self.missing_cols=expected-actual_cols
        if self.missing_cols:
            logging.critical(f'Shapecheck failed. The table is missing the following columns: {self.missing_cols}. Closing program.')
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

class HTML_Extraction(ABC):
    """All classes that will interact with beautifulsoup must inherit this."""
    @property
    @abstractmethod
    def id(self):
        """HTML class identifier"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def expected_cols(self):
        """List or set of expected column names for conducting shapechecks"""
        raise NotImplementedError()

class FactDetails(HTML_Extraction):
    """All classes that will interact with the Fact class must inherit this."""
    @property
    @abstractmethod
    def identifier(self):
        """Unique identifier for the category"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def value_vars(self):
        """List or set of expected column names for pivoting. These will be retained."""
        raise NotImplementedError()

    @property
    @abstractmethod
    def stat_lookup(self):
        """Dictionary of all stats that will be imported, along with their corresponding id."""
        raise NotImplementedError()

class MissingCols(Exception):
    pass

class Stat_Cat(ABCMeta): # any flat class used to define a statistical category must inherit this
    registry = []

    def __new__(cls, name, bases, attrs):
        # Create the class normally
        new_cls = super().__new__(cls, name, bases, attrs)

        # Skip the base abstract class
        if not attrs.get('__abstractmethods__', False):
            # Enforce required attributes
            required_attrs = ['id', 'expected_cols', 'cat']
            for attr in required_attrs:
                if not hasattr(new_cls, attr):
                    raise TypeError(f"Class {name} must define '{attr}'")

            # Register the concrete class
            Stat_Cat.registry.append(new_cls)

        return new_cls

# orchestrators

class HTML_Layer:
    def __init__(self,settings):
        logging.info('Starting the html layer...\n')
        try:
            self.year=settings.year
            self.team_htmls={}
            self.roster_htmls={}
            self.week_htmls={}

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

            if settings.scrape_teams==True or settings.scrape_rosters==True:
                logging.debug('Scraping loop for teams/rosters triggered\n')
                self.extract_teams()

            if settings.scrape_games==True:
                for week in range(settings.start_week,settings.end_week):
                    logging.info(f'Now scraping html for week {week}')
                    self.week_htmls[week]=[]
                    self.url=f'https://www.pro-football-reference.com/years/{settings.year}/week_{week}.htm'
                    week_html=self.load_page()
                    soup=BeautifulSoup(week_html,'html.parser')
                    week_games=soup.find_all('div',class_='game_summaries')
                    if len(week_games)==2:
                        week_games=week_games[1]
                    else:
                        week_games=week_games[0]

                    games=week_games.find_all('div',class_='game_summary expanded nohover')

                    games=games[:1]

                    games_count=len(games)

                    for i, game in enumerate(games):
                        logging.info(f'Scraping game {i} of {games_count}\n')
                        game_link=game.find('td',class_='right gamelink')
                        link=game_link.find('a')['href']
                        self.url=f'https://www.pro-football-reference.com{link}'
                        html=self.load_page()
                        self.week_htmls[week].append(html)
        finally:
            self.driver.quit()
            logging.info('Webdriver sucessfully closed.')

    def extract_teams(self):
        for team in teams:
            logging.info(f'Scraping {team}...\n')
            dicref=teams[team]
            base_url=f'https://www.pro-football-reference.com/teams/{dicref['url']}/'
            team_abbr=dicref['abbr']
            if self.settings.scrape_teams==True:
                logging.debug('Extracting team details')
                self.url=base_url+f'{self.year}_roster.htm'
                roster_html=self.load_page()
                self.roster_htmls[team_abbr]=roster_html
            if self.setting.scrape_rosters==True:
                logging.debug('Extracting roster details...')
                self.url=base_url+f'{self.year}.htm'
                team_html=self.load_page()
                self.team__htmls[team_abbr]=team_html
            logging.debug('Finished\n')

    def load_page(self,attempt=1,max_attempts=3):
        logging.debug(f'attempting to extract html for {self.url}')
        try:
            self.driver.get(self.url)
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        except Exception as e:
            if attempt<max_attempts:
                logging.warning(f'Attempt {attempt} failed- reattempting.')
                return self.load_page(self.url,attempt+1)
            else:
                logging.error(f'Unable to extract {self.url}- attempt 3 failed.')
                raise ExtractionFailed
        try:
            WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "table")))
        except:
            logging.error('Webdriver timed out while waiting for the element.')
            raise Exception
        logging.debug(f'Extraction successful\n\n')
        time.sleep(6) # ensures compliance with PFR's max of 10 requests per minute
        return self.driver.page_source

class default_pipeline_settings:
    start_week=1
    end_week=18
    scrape_rosters=True
    scrape_teams=True
    scrape_games=True

class Season_Mixins:
    def extract_from_html_list(self,element_list,elements):
        target_elements = {
            key: value
            for key, value in vars(elements).items()
            if not key.startswith('__') and not callable(value)
        }

        for attr, idx in target_elements.items():
            element=element_list[idx]
            value=element.get_text().strip()
            value = value.split(":", 1)[-1].strip() if ":" in value else value
            value=value.split("(",1)[0].strip() if "(" in value else value
            setattr(self, attr, value)

def run_pipeline(year):
    settings=default_pipeline_settings
    settings.year=year
    htmls=HTML_Layer(settings)
    obj=Season(htmls,settings)
    return

def join_values(df1,df2):
    df1 = df1[df1['Stat'].isin(summary_stats)]    
    df1 = df1[df1['Player_ID'] == 'bf25f8dd_2024']
    df1 = df1[df1['Tm'].notna()]
    df1=df1[df1['Stat'].str.startswith('P')]

    df2 = df2[df2['Stat'].isin(summary_stats)]    
    df2 = df2[df2['Player_ID'] == 'bf25f8dd_2024']
    df2 = df2[df2['Tm'].notna()]
    df2=df2[df2['Stat'].str.startswith('P')]

    pivot_1 = df1.pivot_table(
    index=['Player_ID'],
    columns='Stat',
    values='Value',  # or 'Value_y'
    fill_value=0
    )

    pivot_2 = df2.pivot_table(
    index=['Player_ID'],
    columns='Stat',
    values='Value',  # or 'Value_y'
    fill_value=0
    )
    combined = (
    pivot_1
    .merge(pivot_2, how='outer',
           left_index=True, right_index=True,
           suffixes=('_x','_y'))
    .fillna(0)
    )

    for col in summary_stats:
        combined[col]=combined[f'{col}_x']+combined[f'{col}_y']

    filtered = combined[[col for col in combined.columns if col in summary_stats]]

    logging.info(f'\n{combined}')
    #print(filtered)

class Season(Season_Mixins):
    def __init__(self,htmls,settings):
        """
        Args:
            htmls: HTML data to process
            settings: Settings object (required)
        """
        self.settings = settings
        self.htmls = htmls
        logging.info(f'\n\nStarting process for the {settings.year} NFL Season.')

        self.year=settings.year
        self.weeks=[]
        
        fact_tables=[]
        dim_games_tables=[]


        if settings.end_week>18:
            logging.debug('End week cannot be greater than 18- setting to 18.')
            settings.end_week=18

        settings.end_week+=1

        start_week=settings.start_week
        end_week=settings.end_week
        self.htmls=htmls

        if settings.scrape_teams is True:
            for team in teams:
                Team(team,htmls)

        if settings.scrape_rosters is True:
            logging.debug('Extracting player tables...')
            Players=DIM_Players(settings.year,htmls)
            self.teamref=Players.df

        for week in range(start_week,end_week):
            week_htmls=self.htmls.week_htmls[str(week)]
            week_obj=Week(week,settings.year,week_htmls)
            week_obj.create_games()
            week_obj.create_tables()
            dim_games_tables.append(week_obj.dim_games)
            week_obj.substitute_player_id(self.teamref)
            fact_tables.append(week_obj.fact_stats)
            self.weeks.append(week_obj)
            if week==1:
                week_obj.season_sum=week_obj.fact_stats
            else:
                last_week=self.weeks[week-2]
                join_values(last_week.fact_stats,week_obj.fact_stats)
        return
        
        self.teamref=self.teamref.drop_duplicates(subset=['Player_ID'])
        self.teamref.drop(columns=['Team'],inplace=True)

        self.FACT_Stats=pd.concat(fact_tables)
        self.DIM_Games=pd.concat(dim_games_tables)

        self.DIM_Games['Year']=settings.year
        self.teamref['Year']=settings.year

        with pd.ExcelWriter('New_Excel_Test.xlsx',mode='w') as writer:
            self.FACT_Stats.to_excel(writer,sheet_name='FACT_Stats',index=False)
            self.DIM_Games.to_excel(writer,sheet_name='DIM_Games',index=False)
            self.teamref.to_excel(writer,sheet_name='DIM_Players',index=False)

class Week:
    def __init__(self,week,year,htmls):
        self.week=week
        self.year=year
        self.htmls=htmls

        self.games=[]
        self.dim_game_rows=[]
        self.fact_tables=[]

    def create_tables(self):
        for game in self.games:
            self.fact_tables.append(game.Fact_Table.df)
            for row in game.game_rows:
                self.dim_game_rows.append(row)

        dim_game_columns=['Team_Tag','Game_id','Team','Opponent','Game_Date','Game_Time','Stadium','ref','surface','roof']
        self.dim_games=pd.DataFrame(self.dim_game_rows,columns=dim_game_columns)

        self.fact_stats=pd.concat(self.fact_tables).reset_index(drop=True)
        self.fact_stats['Value']=self.fact_stats['Value'].str.rstrip('%').astype(float).fillna(0)

    def create_games(self):
        for i,html in enumerate(self.htmls,start=1):
            soup=BeautifulSoup(html,'html.parser')
            game=Game(soup,i,self.year,self.week)
            self.games.append(game)

    def substitute_player_id(self,player_table):
        self.fact_stats=self.fact_stats.merge(
            player_table[['Name','Team','Player_ID']],
            how='left',
            left_on=['Player','Tm'],
            right_on=['Name','Team']
        )
        self.fact_stats.drop(columns=['Player','Name','Team'],inplace=True)
        self.fact_stats=self.fact_stats[['Player_ID','Tm','Game_id','Stat','Value']]

# constants

class Passing(metaclass=Stat_Cat):
    expected_cols={'Player':object,'Tm':object,'Cmp':np.int64,'Att':np.int64,'Yds':np.int64,'1D':np.int64,'1D%':np.float64,'IAY':np.int64,'IAY/PA':np.float64,'CAY':np.int64,'CAY/Cmp':np.float64,'CAY/PA':np.float64,'YAC':np.int64,'YAC/Cmp':np.float64,'Drops':np.int64,'Drop%':np.float64,'BadTh':np.int64,'Bad%':np.float64,'Sk':np.int64,'Bltz':np.int64,'Hrry':np.int64,'Hits':np.int64,'Prss':np.int64,'Prss%':np.float64,'Scrm':np.int64,'Yds/Scr':np.float64}
    value_vars=['Cmp','Att','Yds','1D','1D%','IAY','IAY/PA','CAY','CAY/Cmp','CAY/PA','YAC','YAC/Cmp','Drops','Drop%','BadTh','Bad%','Sk','Bltz','Hrry','Hits','Prss','Prss%','Scrm','Yds/Scr']
    pct=['Drop%','Bad%','Prss%']
    cleaning={
        '%':['Drop%','Bad%','Prss%']
    }
    id='passing_advanced'
    cat='passing'
    identifier='p'
    avg_columns={
        'Avg':['Yds','Att']
    }
    stat_lookup={
        'Cmp':'P1',
        'Att':'P2',
        'Yds':'P3',
        '1D':'P4',
        '1D%':'P5',
        'IAY':'P6',
        'IAY/PA':'P7',
        'CAY':'P8',
        'CAY/Cmp':'P9',
        'CAY/PA':'P10',
        'YAC':'P11',
        'YAC/Cmp':'P12',
        'Drops':'P13',
        'Drop%':'P14',
        'BadTh':'P15',
        'Bad%':'P16',
        'Sk':'P17',
        'Bltz':'P18',
        'Hrry':'P19',
        'Hits':'P20',
        'Prss':'P21',
        'Prss%':'P22',
        'Scrm':'P23',
        'YdsScr':'P24'
    }

class Receiving(metaclass=Stat_Cat):
    expected_cols={'Player':object,'Tm':object,'Tgt':np.int64,'Rec':np.int64,'Yds':np.int64,'TD':np.int64,'1D':np.int64,'YBC':np.int64,'YBC/R':np.float64,'YAC':np.int64,'YAC/R':np.float64,'ADOT':np.float64,'BrkTkl':np.int64,'Rec/Br':np.float64,'Drop':np.int64,'Drop%':np.float64,'Int':np.int64,'Rat':np.float64}
    value_vars=['Tgt','Rec','Yds','TD','1D','YBC','YBC/R','YAC','YAC/R','ADOT','BrkTkl','Rec/Br','Drop','Drop%','Int','Rat']
    id='receiving_advanced'
    cat='receiving'
    identifier='c' # rushing and receiving both start with r, so this has c for catching
    stat_lookup={
        'Tgt':'C1',
        'Rec':'C2',
        'Yds':'C3',
        'TD':'C4',
        '1D':'C5',
        'YBC':'C6',
        'YBC/R':'C7',
        'YAC':'C8',
        'YAC/R':'C9',
        'ADOT':'C10',
        'BrkTkl':'C11',
        'Rec/Br':'C12',
        'Drop':'C13',
        'Drop%':'C14',
        'Int':'C15',
        'Rat':'C16'
    }

class Rushing(metaclass=Stat_Cat):
    expected_cols={'Player':object,'Tm':object,'Att':np.int64,'Yds':np.int64,'TD':np.int64,'1D':np.int64,'YBC':np.int64,'YBC/Att':np.float64,'YAC':np.int64,'YAC/Att':np.float64,'BrkTkl':np.int64,'Att/Br':np.float64}
    value_vars=['Att','Yds','TD','1D','YBC','YBC/Att','YAC','YAC/Att','BrkTkl','Att/Br']
    id='rushing_advanced'
    cat='rushing'
    identifier='r'
    stat_lookup={
        'Att':'R1',
        'Yds':'R2',
        'TD':'R3',
        '1D':'R4',
        'YBC':'R5',
        'YBC/Att':'R6',
        'YAC':'R7',
        'YAC/Att':'R8',
        'BrkTkl':'R9',
        'Att/Br':'R10'
    }

class Defense(metaclass=Stat_Cat):
    expected_cols={'Player':object,'Tm':object,'Int':np.int64,'int_Yds':np.int64,'int_TD':np.int64,'Lng':np.int64,'PD':np.int64,'Sk':np.float64,'Comb':np.int64,'Solo':np.int64,'Ast':np.int64,'TFL':np.int64,'QBHits':np.int64,'FR':np.int64,'Yds':np.int64,'TD':np.int64,'FF':np.int64}
    value_vars=['Int','int_Yds','int_TD','Lng','PD','Sk','Comb','Solo','Ast','TFL','QBHits','FR','Yds','TD','FF','Tgt','Cmp','Cmp%','Yds','Yds/Cmp','Yds/Tgt','TD','Rat','DADOT','Air','YAC','Bltz','Hrry','QBKD','Sk','Prss','Comb','MTkl','MTkl%']
    id='player_defense'
    cat='defense'
    identifier='d'
    cleaning={
        '%':['Cmp%','MTkl%']
    }
    stat_lookup={
        'Int':'D1',
        'int_Yds':'D2',
        'int_TD':'D3',
        'Lng':'D4',
        'PD':'D5',
        'Sk':'D6',
        'Comb':'D7',
        'Solo':'D8',
        'Ast':'D9',
        'TFL':'D10',
        'QBHits':'D11',
        'FR':'D12',
        'Yds':'D13',
        'TD':'D14',
        'FF':'D15',
        'Tgt':'D16',
        'Cmp':'D17',
        'Cmp%':'D18',
        'Yds_Allowed':'D19',   
        'Yds/Cmp':'D20',
        'Yds/Tgt':'D21',
        'TD_Allowed':'D22',
        'Rat':'D23',
        'DADOT':'D24',
        'Air':'D25',
        'YAC':'D26',
        'Bltz':'D27',
        'Hrry':'D28',
        'QBKD':'D29',
        'Prss':'D30',
        'MTkl':'D31',
        'MTkl%':'D32'
    }

class Advanced_Defense(HTML_Extraction): # DO NOT add the stat_cat metaclass to this. This is to set the extraction to be added into the defense table.
    id='defense_advanced'

    cols=['Player','Tm','Int','Tgt','Cmp','Cmp%','Yds','Yds/Cmp','Yds/Tgt','TD','Rat','DADOT','Air','YAC','Bltz','Hrry','QBKD','Sk','Prss','Comb','MTkl','MTkl%']
    expected_cols={col:None for col in cols}

    cat='advanced defense'

# functions

class Game_Details:
    game_date=0
    game_time=1
    stadium=2

class ref_table_targets:
    ref=1

class Game_Dets(Season_Mixins):
    def __init__(self,soup,game_id):
        self.soup=soup

        scorebox=soup.find('div',class_='scorebox')
        self.sects=scorebox.find_all('strong')

        away_team_box=self.sects[0]
        away_team=away_team_box.get_text().strip()

        home_team_box=self.sects[2]
        home_team=home_team_box.get_text().strip()

        self.home_team_key=teams[home_team]['abbr'].upper()
        self.away_team_key=teams[away_team]['abbr'].upper()

        self.team_tags={
            self.home_team_key:f'{game_id}H',
            self.away_team_key:f'{game_id}A'
        }

        game_details_area=soup.find('div',class_='scorebox_meta')
        game_details_list=game_details_area.find_all('div')

        self.extract_from_html_list(game_details_list,Game_Details)

        #self.game_date=game_details[0].get_text().strip()

        game_info_box=soup.find('table',id='game_info')
        
        rows = game_info_box.find_all('td', attrs={'class': 'center', 'data-stat': 'stat'})

        self.extract_from_html_list(rows,Other_Game_Details)

        reftable=soup.find('table',id='officials')
        rows=reftable.find_all('td')
        self.extract_from_html_list(rows,ref_table_targets)
        
        #roofrow=rows[1]
        #self.roof=roofrow.find('td',class_='center').get_text()
        
        #surfacerow=rows[2]
        #self.surface=surfacerow.find('td',class_='center').get_text()
        
        #row=rows[1]
        #self.ref=row.find('td',{'data-stat':'name'}).get_text()

class Other_Game_Details:
    roof=1
    surface=2

class Game:
    def __init__(self,soup,index,year,week):
        self.game_id=f'{week}{index}{year}'

        self.details=Game_Dets(soup,self.game_id)

        base_list=[self.game_id,self.details.game_date,self.details.game_time,self.details.stadium,self.details.ref,self.details.surface,self.details.roof]

        home_list = [f'{self.game_id}H', self.details.home_team_key, self.details.away_team_key] + base_list
        away_list = [f'{self.game_id}A', self.details.away_team_key, self.details.home_team_key] + base_list
        self.game_rows=[home_list,away_list]

        self.df=pd.DataFrame(self.game_rows,columns=['Game_ID','Team','Opponent','Game','Date','Time','Stadium','Ref','Surface','Roof'])

        self.Fact_Table=Fact_Table(soup,self)

# Fact Table functionality

class Fact(Table): #functionality
    def __init__(self,soup,category):
        logging.info(f'Extracting {category.cat} data...')
        for k,v in category.__dict__.items():
            if not k.startswith('__'):
                setattr(self,k,v)
        try:
            super().__init__(category,soup)
        except MissingCols:
            raise MissingCols
        self.df=self.df[self.df['Player']!='Player']
        self.long_now()
        self.clean_and_convert()
        self.df = self.df.pivot_table(
            index=['Player','Tm'],
            columns='Stat',
            values='Value',
            fill_value=0
        )
        for col in category.avg_columns:
            calc_cols=category.avg_columns[col]
            self.df[col]=self.df[calc_cols[0]]/self.df[calc_cols[1]]
        print(self.df)

    def long_now(self):
        
        id_vars = ['Player', 'Tm']
        value_vars = [col for col in self.df.columns if col not in id_vars]

        self.df = self.df.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='Stat',
            value_name='Value'
        )

    def clean_and_convert(self):
        self.df['Value'] = self.df['Value'].str.replace('%','').astype(float)

# Fact_Stats

class Defense_Table(Fact): #extension
    def __init__(self,soup,category):
        self.soup=soup
        try:
            super().__init__(soup,category)
        except MissingCols: # defense table will fail shapecheck on import- shapecheck occurs after renaming duplicate columns
            pass

        box_1=self.df.iloc[:,:2]
        box_2=self.df.iloc[:,2:7].rename(columns={'Yds':'int_Yds','TD':'int_TD'})
        box_3=self.df.iloc[:,7:]

        self.base_defense=pd.concat([box_1,box_2,box_3],axis=1)

        self.df=self.base_defense

        self.shapecheck()

        advanced_stats=self.get_advanced_stats()

        self.df=pd.merge(self.base_defense,advanced_stats,on=['Player','Tm'],how='outer').fillna(0)

        self.df = self.df[self.df['Player'] != 'Player']
        self.df = self.df[self.df['Player'] != 0]
        logging.debug(f'\n{self.df}')

    def get_advanced_stats(self):
        advanced=Table(Advanced_Defense,self.soup)
        advanced.df.drop(columns=['Int','Sk','Comb'],inplace=True)
        advanced.df.rename(columns={'Yds':'Yds_Allowed','TD':'TD_Allowed'},inplace=True)
        return advanced.df

class Fact_Table(Table): # orchestration
    def __init__(self,soup,game_details):
        logging.info('Extracting fact table data...')
        
        dataframes=[]

        for cat_cls in Stat_Cat.registry:
            if cat_cls.cat=='defense':
                instance=Defense_Table(soup,cat_cls)
            else:
                instance=Fact(soup,cat_cls)
            #if cat_cls.cat=='passing':
            #    print(instance.df)
            #instance.long_now()
            dataframes.append(instance.df)
        self.df=pd.concat(dataframes)

        self.df['Game_id']=self.df['Tm'].map(game_details.details.team_tags)
        self.df=self.df[['Player','Tm','Game_id','Stat','Value']]

# Dimension Tables

# DIM_Players

class Roster(HTML_Extraction):
    id='roster'
    expected_cols={'No.':object,'Player':object,'Age':np.int64,'Pos':object,'G':np.int64,'GS':np.int64,'Wt':object,'Ht':object,'College/Univ':object,'BirthDate':object,'Yrs':object,'AV':object,'Drafted (tm/rnd/yr)':object}
    cleaning={
        ',':{'cols':['College/Univ'],'replace':'/'},
    }
    cat='DIM_Players'

class Players_Table(Table):
    def __init__(self,soup,year):
        self.year=year
        self.soup=soup
        super().__init__(Roster,soup)
        self.df=self.df[self.df['No.'] != 'No.'].reset_index(drop=True)
        #self.process()
        self.df.drop(columns=['Drafted (tm/rnd/yr)'],inplace=True)
        self.df['Yrs']=self.df['Yrs'].replace('Rook', 0)
        starters=self.get_starters()
        self.df['Starter']=self.df['Player'].isin(starters)
    
    def get_starters(self):
        df=ExtractTable(self.soup,'starters')
        df['Player']=df['Player'].str.replace('*','').fillna(0)
        df=df[df['Pos'] != ''].reset_index(drop=True)
        my_list=df['Player'].tolist()
        return my_list

class DIM_Players(DIM_Players_Mixin):
    def __init__(self,year,htmls):
        self.year=year
        self.dfs={}

        for team in teams:
            html=htmls.roster_htmls[teams[team]['abbr']]
            soup=BeautifulSoup(html,'html.parser')
            table=Players_Table(soup,year)
            self.df=table.df.copy()  # ensure it's a copy
            self.generate_player_id(self.df['Player'],self.df['BirthDate'])
            self.df['Team']=teams[team]['abbr']  # now safe to assign
            self.dfs[teams[team]['abbr']]=self.df
        self.df=pd.concat(self.dfs)
        
        cols = self.df.columns.tolist()
        for col in ['Player_ID','Player'][::-1]:
            cols.insert(0, cols.pop(cols.index(col)))
        self.df = self.df[cols]

        logging.info(self.df)

# DIM_Teams

class Team_Details_1:
    head_coach=1
    offensive_coordinator=6
    defensive_coordinator=7
    stadium=9

class Team_Details_2:
    head_coach=1
    offensive_coordinator=7
    defensive_coordinator=8
    stadium=10


class Team(Table,Season_Mixins):
    def __init__(self,team,htmls):
        team_abbr=teams[team]['abbr']
        html=htmls.team_htmls[team_abbr]
        soup=BeautifulSoup(html,'html.parser')
        
        team_details_area=soup.find('div',{'data-template':'Partials/Teams/Summary'})
        team_details=team_details_area.find_all('p')

        if len(team_details)==16:
            self.extract_from_html_list(team_details,Team_Details_1)
        elif len(team_details)==17:

            self.extract_from_html_list(team_details,Team_Details_2)
        
        return
        team_dets=soup.find('div',{'data-template':'Partials/Teams/Summary'})
        details=team_dets.find_all('p')
        head_coach_line=details[1]
        head_coach=head_coach_line.find('a').get_text()
        oc_line=details[7]
        oc=oc_line.find('a').get_text()
        dc_line=details[8]
        dc=dc_line.find('a').get_text()
        owner_line=details[12]
        owner=owner_line.find('a').get_text()
        sos_line=details[5]
        sos_line_dets=sos_line.find_all('a')
        sos_box=sos_line_dets[1]
        sos=sos_box.find('a').get_text()

# helpers

class Scraper_Settings:
    def __init__(self,rosters,teams,games,start_week,end_week):
        self.scrape_rosters=rosters
        self.scrape_teams=teams
        self.scrape_games=games
        self.start_week=start_week
        self.end_week=end_week
