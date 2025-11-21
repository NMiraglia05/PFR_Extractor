from abc import ABC, ABCMeta, abstractmethod
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
from extractor import ExtractTable, DIM_Players_Mixin, Table, Fact, HTML_Scraper
import logging
import json

logging.basicConfig(
    filename=f'logs/log_{date.today()}.txt',
    level=logging.INFO,
    format='%(levelname)s - %(message)s',
    filemode='w'
    )

with open("teams.json") as f:
    teams = json.load(f)

# base classes

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
            required_attrs = ['id', 'expected_cols', 'cat', 'col_order', 'value_vars', 'identifier', 'stat_lookup']
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

            scraper=HTML_Scraper()

            if settings.scrape_teams==True or settings.scrape_rosters==True:
                logging.debug('Scraping loop for teams/rosters triggered\n')
                self.extract_teams()

            if settings.scrape_games==True:
                print('aaaaaaaaaaa')
                print(settings.start_week,settings.end_week)
                for week in range(settings.start_week,settings.end_week+1):
                    logging.info(f'Now scraping html for week {week}\n')
                    self.week_htmls[week]=[]
                    self.url=f'https://www.pro-football-reference.com/years/{settings.year}/week_{week}.htm'
                    logging.debug(f'Week URL: {self.url}')
                    week_html=scraper.scrape(self.url)
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
                        html=scraper.scrape(self.url)
                        self.week_htmls[week].append(html)
        finally:
            scraper.quit()
            logging.info('Scraper quit.\n')

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

class default_pipeline_settings:
    start_week=2
    end_week=2
    scrape_rosters=False
    scrape_teams=False
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
    logging.info('Initializing pipeline...\n')
    settings=default_pipeline_settings
    settings.year=year
    #logging.debug(f'Settings:\n\n{settings.__dict__()}\n')
    htmls=HTML_Layer(settings)
    obj=Season(htmls,settings)
    return

class Season(Season_Mixins):
    def __init__(self,htmls,settings):
        self.settings = settings
        self.htmls = htmls
        logging.info(f'Starting process for the {settings.year} NFL Season.\n\n')

        self.year=settings.year
        self.weeks=[]
        
        fact_tables=[]
        dim_games_tables=[]


        if settings.end_week>18:
            logging.debug('End week cannot be greater than 18- setting to 18.')
            settings.end_week=18

        settings.end_week+=1 #ensures users can specify their actual desired endweek. no need to understand how the Range loop works

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
            week_id=f'W{week}{settings.year}'
            week_htmls=self.htmls.week_htmls[week]
            week_obj=Week(week,settings.year,week_htmls)
            weekrow = [week_id, 'Season Sum']
            cols = week_obj.dim_games.columns
            row = {col: None for col in cols}
            row[cols[0]] = week_id
            row[cols[1]] = 'Season Sum'

            week_obj.dim_games = pd.concat(
                [week_obj.dim_games, pd.DataFrame([row])],
                ignore_index=True
            )
            dim_games_tables.append(week_obj.dim_games)
            #week_obj.substitute_player_id(self.teamref)
            fact_tables.append(week_obj.fact_stats)
            print(fact_tables)
            return
            self.weeks.append(week_obj)
            if week==1:
                week_obj.season_sum=week_obj.fact_stats
            else:
                last_week=self.weeks[week-2]
                giggidy=self.join_values(last_week.season_sum,week_obj.fact_stats)
                week_obj.season_sum=giggidy
                week_obj.season_sum['Game_ID']=week_id
            fact_tables.append(week_obj.season_sum)
        return

        self.teamref=self.teamref.drop_duplicates(subset=['Player_ID'])
        self.teamref.drop(columns=['Team'],inplace=True)

        self.FACT_Stats=pd.concat(fact_tables)
        self.DIM_Games=pd.concat(dim_games_tables)

        self.DIM_Games['Year']=settings.year
        self.teamref['Year']=settings.year

        return

        with pd.ExcelWriter('New_Excel_Test.xlsx',mode='w') as writer:
            self.FACT_Stats.to_excel(writer,sheet_name='FACT_Stats',index=False)
            self.DIM_Games.to_excel(writer,sheet_name='DIM_Games',index=False)
            self.teamref.to_excel(writer,sheet_name='DIM_Players',index=False)

    def pivot_and_combine(self,df_list,cat):
        pivoted_dfs=[]
        for df in df_list:
            df = df[df['Stat'].isin(cat.summary_stats)]    
            df = df[df['Tm'].notna()]
            df=df[df['Stat'].str.startswith(cat.identifier)]

            pivot = df.pivot_table(
            index=['Player_ID','Tm'],
            columns='Stat',
            values='Value',  # or 'Value_y'
            fill_value=0
            )
            pivoted_dfs.append(pivot)
            logging.info(f'Pivot:\n\n{pivot}')

        pivot1=pivoted_dfs[0]
        pivot2=pivoted_dfs[1]

        combined = (
            pivot1
            .merge(pivot2, how='outer',
                left_index=True, right_index=True,
                suffixes=('_x','_y'))
            .fillna(0)
            )
        return combined

    def join_values(self,season_sum,current_week):
        df_list=[season_sum,current_week]
        long_dfs=[]
        
        for cat in Stat_Cat.registry:
            combined=self.pivot_and_combine(df_list,cat)
            logging.info(f'\ncinema\n\n{combined}')
            for calc in cat.season_calcs:
                if calc=='sum':
                    for col in cat.season_calcs[calc]:
                        combined[f'{col}_z'] = combined[f'{col}_x'] + combined[f'{col}_y']
                
                if calc=='avg':
                    for col in cat.season_calcs[calc]:
                        dicref=cat.season_calcs[calc][col]
                        combined[f'{col}_z']=(combined[f'{dicref[0]}_x']+combined[f'{dicref[0]}_y'])/(combined[f'{dicref[1]}_x']+combined[f'{dicref[1]}_y'])

                if calc=='pct':
                    for col in cat.season_calcs[calc]:
                        dicref=cat.season_calcs[calc][col]
                        combined[f'{col}_z']=((combined[f'{dicref[0]}_x']+combined[f'{dicref[0]}_y'])*100)/(combined[f'{dicref[1]}_x']+combined[f'{dicref[1]}_y'])

                if calc=='rat':
                    for col in cat.season_calcs[calc]:
                        combined[f'{col}_z']=(combined[f'{col}_x']+combined[f'{col}_y'])/2

            pivot_2 = combined[[col for col in combined.columns if '_z' in col]]
            pivot_2 = pivot_2.rename(columns=lambda x: x.replace('_z', ''))
            pivot_2 = pivot_2.reset_index()

            melted=pd.melt(pivot_2,id_vars=['Player_ID','Tm'],value_vars=cat.season_vals,var_name='Stat')
            long_dfs.append(melted)
        season_sum=pd.concat(long_dfs)
        return season_sum

class Week:
    def __init__(self,week,year,htmls):
        self.week=week
        self.year=year
        self.htmls=htmls

        self.games=[]
        self.dim_game_rows=[]
        self.fact_tables=[]

        self.create_games()
        self.create_tables()

    def create_tables(self):
        for game in self.games:
            self.fact_tables.append(game.Fact_Table.df)
            for row in game.game_rows:
                self.dim_game_rows.append(row)

        dim_game_columns=['Team_Tag','Game_id','Team','Opponent','Game_Date','Game_Time','Stadium','ref','surface','roof']
        self.dim_games=pd.DataFrame(self.dim_game_rows,columns=dim_game_columns)

        self.fact_stats=pd.concat(self.fact_tables).reset_index(drop=True)

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
        self.fact_stats=self.fact_stats[['Player_ID','Tm','Game_id','Stat','Value']].fillna(0)

# constants

class Receiving(metaclass=Stat_Cat):
    expected_cols={'Player':object,'Tm':object,'Tgt':np.int64,'Rec':np.int64,'Yds':np.int64,'TD':np.int64,'1D':np.int64,'YBC':np.int64,'YBC/R':np.float64,'YAC':np.int64,'YAC/R':np.float64,'ADOT':np.float64,'BrkTkl':np.int64,'Rec/Br':np.float64,'Drop':np.int64,'Drop%':np.float64,'Int':np.int64,'Rat':np.float64}
    value_vars=['Tgt','Rec','Pct','Yds','Avg/R','TD','1D','YBC','YBC/R','YAC','YAC/R','ADOT','BrkTkl','Rec/Br','Drop','Drop%','Int','Rat']
    col_order=['Player','Tm','Tgt','Rec','Pct','Yds','Avg/R','TD','1D','YBC','YBC/R','YAC','YAC/R','ADOT','BrkTkl','Rec/Br','Drop','Drop%','Int','Rat']
    id='receiving_advanced'
    cat='receiving'
    identifier='C' # rushing and receiving both start with r, so this has c for catching
    stat_lookup={
            'Tgt':'C1',
            'Rec':'C2',
            'Pct':'C3',
            'Yds':'C4',
            'Avg/R':'C5',
            'TD':'C6',
            '1D':'C7',
            'YBC':'C8',
            'YBC/R':'C9',
            'YAC':'C10',
            'YAC/R':'C11',
            'ADOT':'C12',
            'BrkTkl':'C13',
            'Rec/Br':'C14',
            'Drop':'C15',
            'Drop%':'C16',
            'Int':'C17',
            'Rat':'C18'
        }
    calc_columns={
        'avg':{
            'Avg/R':['Yds','Rec']
            },
        'pct':{
            'Pct':['Tgt','Rec']
            }
        }

    season_calcs={
        'sum':['C1','C2','C4','C6','C7','C8','C10','C13','C15','C17'],
        'avg':{'C5':['C4','C2'],'C9':['C8','C2'],'C11':['C10','C2'],'C14':['C2','C13']},
        'pct':{'C3':['C2','C1'],'C16':['C15','C1']},
        'rat':['C12','C18']
        }
    summary_stats=['C1','C2','C4','C6','C7','C8','C10','C12','C13','C15','C16','C17','C18']

    season_vals=['C1','C2','C4','C6','C7','C8','C10','C13','C15','C17',
        'C5','C9','C11','C14','C3','C16','C12','C18']

class Passing(metaclass=Stat_Cat):
    expected_cols={'Player':object,'Tm':object,'Cmp':np.int64,'Att':np.int64,'Yds':np.int64,'1D':np.int64,'1D%':np.float64,'IAY':np.int64,'IAY/PA':np.float64,'CAY':np.int64,'CAY/Cmp':np.float64,'CAY/PA':np.float64,'YAC':np.int64,'YAC/Cmp':np.float64,'Drops':np.int64,'Drop%':np.float64,'BadTh':np.int64,'Bad%':np.float64,'Sk':np.int64,'Bltz':np.int64,'Hrry':np.int64,'Hits':np.int64,'Prss':np.int64,'Prss%':np.float64,'Scrm':np.int64,'Yds/Scr':np.float64}
    value_vars=['Cmp','Att','Yds','Avg','Pct','1D','1D%','IAY','IAY/PA','CAY','CAY/Cmp','CAY/PA','YAC','YAC/Cmp','Drops','Drop%','BadTh','Bad%','Sk','Bltz','Hrry','Hits','Prss','Prss%','Scrm','Yds/Scr','PassPlays']
    col_order=['Player','Tm','Cmp','Att','Yds','Avg','Pct','1D','1D%','IAY','IAY/PA','CAY','CAY/Cmp','CAY/PA','YAC','YAC/Cmp','Drops','Drop%','BadTh','Bad%','Sk','Bltz','Hrry','Hits','Prss','Prss%','Scrm','ScrmYds','Yds/Scr','PassPlays']
    cleaning = {
        'Drop%': [{'target': '%', 'replace_with': ''}],
        'Bad%': [{'target': '%', 'replace_with': ''}],
        'Prss%': [{'target': '%', 'replace_with': ''}]
            }
    id='passing_advanced'
    cat='passing'
    identifier='P'
    calc_columns={
        'avg':{
            'Avg':['Yds','Att']
            },
        'pct':{
            'Pct':['Cmp','Att']
            },
        'tot':{
            'ScrmYds':['Yds/Scr','Scrm']
        },
        'sum':{
            'PassPlays':['Att','Sk']
            }
        }
    summary_stats=['P1','P2','P3','P6','P8','P10','P13','P15','P17','P19','P20','P21','P22','P23','P25','P26','P28']
    season_calcs={
        'sum':['P1','P2','P3','P6','P8','P10','P13','P15','P17','P19','P20','P21','P22','P23'],
        'avg':{'P4':['P3','P2'],'P9':['P8','P2'],'P11':['P10','P1'],'P12':['P10','P2'],'P14':['P13','P1']},
        'pct':{'P5':['P1','P2'],'P7':['P6','P28'],'P16':['P15','P2'],'P18':['P17','P2']}
    }
    stat_lookup={
        'Cmp':'P1',
        'Att':'P2',
        'Yds':'P3',
        'Avg':'P4',
        'Pct':'P5',
        '1D':'P6',
        '1D%':'P7',
        'IAY':'P8',
        'IAY/PA':'P9',
        'CAY':'P10',
        'CAY/Cmp':'P11',
        'CAY/PA':'P12',
        'YAC':'P13',
        'YAC/Cmp':'P14',
        'Drops':'P15',
        'Drop%':'P16',
        'BadTh':'P17',
        'Bad%':'P18',
        'Sk':'P19',
        'Bltz':'P20',
        'Hrry':'P21',
        'Hits':'P22',
        'Prss':'P23',
        'Prss%':'P24',
        'Scrm':'P25',
        'ScrmYds':'P26',
        'Yds/Scr':'P27',
        'PassPlays':'P28'
        }
    
    season_vals=['P1','P2','P3','P6','P8','P10','P13','P15','P17','P19','P20','P21','P22','P23',
        'P4','P9','P11','P12','P14','P5','P7','P16','P18']

class Rushing(metaclass=Stat_Cat):
    expected_cols={'Player':object,'Tm':object,'Att':np.int64,'Yds':np.int64,'TD':np.int64,'1D':np.int64,'YBC':np.int64,'YBC/Att':np.float64,'YAC':np.int64,'YAC/Att':np.float64,'BrkTkl':np.int64,'Att/Br':np.float64}
    value_vars=['Att','Yds','Avg/A','TD','1D','YBC','YBC/Att','YAC','YAC/Att','BrkTkl','Att/Br']
    col_order=['Player','Tm','Att','Yds','Avg/A','TD','1D','YBC','YBC/Att','YAC','YAC/Att','BrkTkl','Att/Br']
    id='rushing_advanced'
    cat='rushing'
    identifier='R'
    stat_lookup={
            'Att':'R1',
            'Yds':'R2',
            'Avg/A':'R3',
            'TD':'R4',
            '1D':'R5',
            'YBC':'R6',
            'YBC/Att':'R7',
            'YAC':'R8',
            'YAC/Att':'R9',
            'BrkTkl':'R10',
            'Att/Br':'R11'
        }
    calc_columns={
        'avg':{
            'Avg/A':['Yds','Att']
        }
    }
    season_calcs={
        'sum':['R1','R2','R4','R5','R6','R8','R10'],
        'avg':{'R3':['R2','R1'],'R7':['R6','R1'],'R9':['R8','R1'],'R11':['R1','R10']}
    }
    summary_stats=['R1','R2','R4','R5','R6','R8','R10']

    season_vals=['R1','R2','R4','R5','R6','R8','R10','R3','R7','R9','R11']

class Defense():
    expected_cols={'Player':object,'Tm':object,'Int':np.int64,'int_Yds':np.int64,'int_TD':np.int64,'Lng':np.int64,'PD':np.int64,'Sk':np.float64,'Comb':np.int64,'Solo':np.int64,'Ast':np.int64,'TFL':np.int64,'QBHits':np.int64,'FR':np.int64,'Yds':np.int64,'TD':np.int64,'FF':np.int64}
    value_vars=['Int','int_Yds','int_TD','Lng','PD','Sk','Comb','Solo','Ast','TFL','QBHits','FR','Yds','TD','FF','Tgt','Cmp','Cmp%','Yds','Yds/Cmp','Yds/Tgt','TD','Rat','DADOT','Air','YAC','Bltz','Hrry','QBKD','Sk','Prss','Comb','MTkl','MTkl%']
    col_order=['Player','Tm','Int','int_Yds','int_TD','Lng','PD','Sk','Comb','Solo','Ast','TFL','QBHits','FR','Yds','TD','FF','Tgt','Cmp','Cmp%','Yds','Yds/Cmp','Yds/Tgt','TD','Rat','DADOT','Air','YAC','Bltz','Hrry','QBKD','Sk','Prss','Comb','MTkl','MTkl%']
    id='player_defense'
    cat='defense'
    identifier='d'
    cleaning = {
        'Cmp%': [
            {'target': '%', 'replace_with': ''}
        ],
        'MTkl%': [
            {'target': '%', 'replace_with': ''}
        ]
        }
    calc_columns={}
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

    season_calcs={
        'sum':['D1','D2','D3','D5','D6','D7','D8','D9','D10','D11','D12','D13','D14','D15','D16','D17','D19','D22','D23','D24','D25','D26','D27','D28','D29','D30','D31'],
        'avg':{'D20':['D13','D17'],'D21':['D13','D16']},
        'pct':{'D18':['D17','D16'],}
        }
    summary_stats=['D1','D2','D3','D5','D6','D7','D8','D9','D10','D11','D12','D13','D14','D15','D16','D17','D19','D22','D23','D24','D25','D26','D27','D28','D29','D30','D31']

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

class Stat_Table(Fact):
    def __init__(self,soup,category):
        self.category=category
        logging.info(f'Extracting {category.cat} data...')
        for k,v in category.__dict__.items():
            if not k.startswith('__'):
                setattr(self,k,v)
        try:
            super().__init__(category,soup)
        except MissingCols:
            raise MissingCols

        self.df=self.df[self.df['Player']!='Player'].fillna(0)

# Fact_Stats

class Defense_Table(Stat_Table): #extension
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
                instance=Stat_Table(soup,cat_cls)
            instance.calculate_values()
            instance.long_now()
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

        logging.debug(self.df)

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
