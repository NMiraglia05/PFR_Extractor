from abc import ABC, abstractmethod
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
import sys
import logging
from datetime import date
from extractor import load_page, ExtractTable, ExtractionFailed
import hashlib

logging.basicConfig(
    filename=f'logs/log_{date.today()}.txt',
    level=logging.INFO,
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
        'abbr':'RAV'
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
        'abbr':'SDG'
    },
    'Denver Broncos':{
        'url':'den',
        'abbr':'DEN'
    },
    'Las Vegas Raiders':{
        'url':'rai',
        'abbr':'RAI'
    },
    'Houston Texans':{      #AFC South
        'url':'htx',
        'abbr':'HTX'
    },
    'Indianapolis Colts':{
        'url':'clt',
        'abbr':'CLT'
    },
    'Tennessee Titans':{
        'url':'oti',
        'abbr':'OTI'
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
        'abbr':'RAM'
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

class Passing(FactDetails):
    expected_cols={'Player':object,'Tm':object,'Cmp':np.int64,'Att':np.int64,'Yds':np.int64,'1D':np.int64,'1D%':np.float64,'IAY':np.int64,'IAY/PA':np.float64,'CAY':np.int64,'CAY/Cmp':np.float64,'CAY/PA':np.float64,'YAC':np.int64,'YAC/Cmp':np.float64,'Drops':np.int64,'Drop%':np.float64,'BadTh':np.int64,'Bad%':np.float64,'Sk':np.int64,'Bltz':np.int64,'Hrry':np.int64,'Hits':np.int64,'Prss':np.int64,'Prss%':np.float64,'Scrm':np.int64,'Yds/Scr':np.float64}
    value_vars=['Cmp','Att','Yds','1D','1D%','IAY','IAY/PA','CAY','CAY/Cmp','CAY/PA','YAC','YAC/Cmp','Drops','Drop%','BadTh','Bad%','Sk','Bltz','Hrry','Hits','Prss','Prss%','Scrm','Yds/Scr']
    pct=['Drop%','Bad%','Prss%']
    cleaning={
        '%':['Drop%','Bad%','Prss%']
    }
    id='passing_advanced'
    cat='passing'
    identifier='p'
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

class Receiving(FactDetails):
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

class Rushing(FactDetails):
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

class Defense(FactDetails):
    expected_cols={'Player':object,'Tm':object,'Int':np.int64,'int_Yds':np.int64,'int_TD':np.int64,'Lng':np.int64,'PD':np.int64,'Sk':np.float64,'Comb':np.int64,'Solo':np.int64,'Ast':np.int64,'TFL':np.int64,'QBHits':np.int64,'FR':np.int64,'Yds':np.int64,'TD':np.int64,'FF':np.int64,'Tgt':np.int64,'Cmp':np.int64,'Cmp%':np.float64,'Yds_Allowed':np.int64,'Yds/Cmp':np.float64,'Yds/Tgt':np.float64,'TD_Allowed':np.int64,'Rat':np.float64,'DADOT':np.float64,'Air':np.float64,'YAC':np.int64,'Bltz':np.int64,'Hrry':np.int64,'QBKD':np.int64,'Sk':np.int64,'Prss':np.int64,'Comb':np.int64,'MTkl':np.int64,'MTkl%':np.float64}
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
        'Sk':'D30',
        'Prss':'D31',
        'Comb':'D32',
        'MTkl':'D33',
        'MTkl%':'D34'
    }

class Game:
    def __init__(self,soup,index,year,week):
        self.game_id=f'{week}{index}{year}'
        scorebox=soup.find('div',class_='scorebox')
        sects=scorebox.find_all('strong')

        away_team_box=sects[0]
        away_team=away_team_box.get_text().strip()

        home_team_box=sects[2]
        home_team=home_team_box.get_text().strip()

        home_team_key=teams[home_team]['abbr'].upper()
        away_team_key=teams[away_team]['abbr'].upper()

        game_details=soup.find('div',class_='scorebox_meta')
        game_details=game_details.find_all('div')
        self.game_date=game_details[0].get_text().strip()

        time_box=game_details[1].get_text().strip()
        self.game_time=time_box.split(':',1)[1].strip()

        stadium_box=game_details[2].get_text().strip()
        self.stadium=stadium_box.split(':',1)[1].strip()

        self.team_tags={
            home_team_key:f'{self.game_id}H',
            away_team_key:f'{self.game_id}A'
        }

        home_list=[f'{self.game_id}H',self.game_id,home_team_key,away_team_key,self.game_date,self.game_time,self.stadium]
        away_list=[f'{self.game_id}A',self.game_id,away_team_key,home_team_key,self.game_date,self.game_time,self.stadium]
        self.game_rows=[home_list,away_list]

        self.Fact_Table=FactTable(soup,self)

class Table:
    """Mixin for validating and cleaning dataframes"""
    def __init__(self):
        try:
            self.shapecheck()
        except MissingCols:
            sys.exit()
        self.clean_table()

    def clean_table(self):
        return
        if hasattr(self, 'cleaning'):
            for dirtychar in self.cleaning:
                cleanchar=self.cleaning[dirtychar]['replace']
                for col in self.cleaning[dirtychar]['cols']:
                    self.df[col]=self.df[col].str.replace(dirtychar,cleanchar).fillna(0)

    def shapecheck(self):
        logging.debug('Conducting shapecheck.')
        actual_cols=set(self.df.columns)
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

class FactTable:
    def __init__(self,soup,game_details):
        logging.info('Extracting fact table data...')
        passing=Fact(soup,Passing)
        receiving=Fact(soup,Receiving)
        rushing=Fact(soup,Rushing)
        defense=Defense_Table(soup,Defense)

        categories=[passing,receiving,rushing,defense]
        dataframes=[]
        
        for cat in categories:
            cat.process()
            dataframes.append(cat.df)

        self.df=pd.concat(dataframes)
        self.df['Game_id']=self.df['Tm'].map(game_details.team_tags)
        self.df=self.df[['Player','Tm','Game_id','Stat','Value']]   

class Fact(Table):
    def __init__(self,soup,category):
        logging.info(f'Extracting {category.cat} data...')
        for k,v in category.__dict__.items():
            if not k.startswith('__'):
                setattr(self,k,v)
        self.df=ExtractTable(soup,self.id).fillna(0).replace('',0)

    def process(self):
        super().__init__()
        
        self.df = self.df[self.df['Player'] != 'Player']

        self.typecheck()

        self.df=self.df.melt(id_vars=['Player','Tm'],value_vars=self.value_vars,var_name='Stat',value_name='Value')

        self.df['Stat']=self.df['Stat'].map(self.stat_lookup)

class Defense_Table(Fact):
    def __init__(self,soup,category):
        self.soup=soup
        super().__init__(soup,category)

        box_1=self.df.iloc[:,:2]
        box_2=self.df.iloc[:,2:7]
        box_3=self.df.iloc[:,7:]
    
        box_2.rename(columns={'Yds':'int_Yds','TD':'int_TD'},inplace=True)

        advanced_stats=self.get_advanced_stats()

        self.df=pd.concat([box_1,box_2,box_3],axis=1)

        self.df = self.df[self.df['Player'] != 0]

        self.df=pd.merge(self.df,advanced_stats,on=['Player','Tm'],how='outer').fillna(0)

    def get_advanced_stats(self):
        df=ExtractTable(self.soup,'defense_advanced').fillna(0).replace('',0)
        df.drop(columns=['Int','Sk','Comb'],inplace=True)
        df.rename(columns={'Yds':'Yds_Allowed','TD':'TD_Allowed'},inplace=True)
        return df

class Season:
    def __init__(self,year,start_week=1,end_week=18,scrape_players=True):
        if scrape_players is True:
            pass
            #Players=DIM_Players(year)

        if end_week>18:
            logging.debug('End week cannot be greater than 18- setting to 18.')
            end_week=18
        self.year=year
        self.weeks=[]
        for week in range(start_week,end_week+1):
            week_obj=Week(week,year)
            self.weeks.append(week_obj)
        
        try:
            for week in self.weeks: #separate loop to avoid keeping webdriver open while soup objects are created
                week.extract_htmls()

        finally:
            logging.info('\n\nClosing webdriver...\n')
            #driver.quit()

        fact_tables=[]

        for week in self.weeks:
            week.create_games()
            week.create_tables()
            fact_tables.append(week.fact_stats)

        self.FACT_Stats=pd.concat(fact_tables)
        print(self.FACT_Stats)

class Week:
    def __init__(self,week,year):
        self.week=week
        self.year=year
        self.week_url=f'https://www.pro-football-reference.com/years/{year}/week_{week}.htm'
        html=load_page(self.week_url)
        self.soup=BeautifulSoup(html,'html.parser')

        self.games=[]
        self.dim_game_rows=[]
        self.fact_tables=[]

        self.links=[]
        self.htmls=[]

        self.extract_links()

    def create_tables(self):
        for game in self.games:
            self.fact_tables.append(game.Fact_Table.df)
            for row in game.game_rows:
                self.dim_game_rows.append(row)

        dim_game_columns=['Team_Tag','Game_id','Team','Opponent','Game_Date','Game_Time','Stadium']
        self.dim_games=pd.DataFrame(self.dim_game_rows,columns=dim_game_columns)

        self.fact_stats=pd.concat(self.fact_tables).reset_index(drop=True)

    def create_games(self):
        for i,html in enumerate(self.htmls,start=1):
            soup=BeautifulSoup(html,'html.parser')
            game=Game(soup,i,self.year,self.week)
            self.games.append(game)

    def extract_htmls(self):
        for link in self.links:
            page_html=load_page(link)
            self.htmls.append(page_html)

    def extract_links(self):
        week_games=self.soup.find_all('div',class_='game_summaries')
        if len(week_games)==2:
            week_games=week_games[1]
        else:
            week_games=week_games[0]

        games=week_games.find_all('div',class_='game_summary expanded nohover')

        for game in games:
            game_link=game.find('td',class_='right gamelink')
            link=game_link.find('a')['href']
            self.links.append(f'https://www.pro-football-reference.com{link}')

class Dimension(Table):
    def __init__(self,soup,category):
        for k,v in category.__dict__.items():
            if not k.startswith('__'):
                setattr(self,k,v)
        self.df=ExtractTable(soup,self.id).fillna(0).replace('',0)

    def process(self):
        super().__init__()
        
        self.df = self.df[self.df['Player'] != 'Player']

        self.typecheck()

class Roster(HTML_Extraction):
    id='roster'
    expected_cols={'No.':object,'Player':object,'Age':np.int64,'Pos':object,'G':np.int64,'GS':np.int64,'Wt':object,'Ht':object,'College/Univ':object,'BirthDate':object,'Yrs':object,'AV':object,'Drafted (tm/rnd/yr)':object}
    cleaning={
        ',':{'cols':['College/Univ'],'replace':'/'},
    }
    cat='DIM_Players'

class Players_Table(Dimension):
    def __init__(self,soup):
        settings=Roster
        self.soup=soup
        super().__init__(soup,settings)
        self.df=self.df[self.df['No.'] != 'No.'].reset_index(drop=True)
        self.process()
        self.df.drop(columns=['Drafted (tm/rnd/yr)'],inplace=True)
        self.df['Yrs']=self.df['Yrs'].replace('Rook', 0)
        starters=self.get_starters()
        self.df['Starter']=self.df['Player'].isin(starters)
    
    def get_starters(self):
        pass
        df=ExtractTable(self.soup,'starters')
        df['Player']=df['Player'].str.replace('*','').fillna(0)
        df=df[df['Pos'] != ''].reset_index(drop=True)
        my_list=df['Player'].tolist()
        return my_list

class MissingCols(Exception):
    pass

class DIM_Players:
    def __init__(self,year):
        self.dfs=[]
        for team in teams:
            url_tag=teams[team]['url']
            url=f'https://www.pro-football-reference.com/teams/{url_tag}/{year}_roster.htm'
            try:
                html=load_page(url)
            except ExtractionFailed:
                continue
            soup=BeautifulSoup(html,'html.parser')
            table=Players_Table(soup)
            self.dfs.append(table.df)
        self.df=pd.concat(self.dfs)
        self.df=self.df.drop_duplicates(subset=['Player','Pos'])
        self.df['Player_ID'] = self.df['Player'].apply(lambda x: int(hashlib.md5(x.encode()).hexdigest()[:8], 16))
        
        self.df['Year_ID'] = self.df['Player_ID'].astype(str) + '_' + str(year)
        cols = self.df.columns.tolist()
        for col in ['Player_ID','Year_ID'][::-1]:
            cols.insert(0, cols.pop(cols.index(col)))
        self.df = self.df[cols]
        
        print(self.df)
        logging.info(self.df)
