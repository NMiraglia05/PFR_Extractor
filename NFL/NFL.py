import re
from abc import ABC, ABCMeta, abstractmethod
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from datetime import date
from extractor import ExtractTable, DIM_Players_Mixin, Table, Fact
import logging
import json
import scraping

logging.basicConfig(
    filename=f'logs/log_{date.today()}.txt',
    level=logging.INFO,
    format='%(levelname)s - %(message)s',
    filemode='w'
    )

with open("teams.json") as f:
    teams = json.load(f)
    teams_df = pd.DataFrame.from_dict(teams, orient='index').reset_index()

with open("stats.json") as f:
    stats_dict = json.load(f)
dim_stats={}

for cat in stats_dict:
    dim_stats[cat]={}
    for item in stats_dict[cat]:
        dim_stats[cat][item['Abbrev']]=item['ID']

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
    
    @property
    @abstractmethod
    def cat(self):
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
        new_cls = super().__new__(cls, name, bases, attrs)

        if not attrs.get('__abstractmethods__', False):
            required_attrs = ['id', 'expected_cols', 'cat', 'col_order', 'value_vars', 'identifier', 'stat_lookup']
            for attr in required_attrs:
                if not hasattr(new_cls, attr):
                    raise TypeError(f"Class {name} must define '{attr}'")

            Stat_Cat.registry.append(new_cls)

        return new_cls

# orchestrators

class HTML_Layer:
    def __init__(self,settings):
        logging.info('Starting the html layer...\n')
        self.settings=settings
        self.scraper=scraping.Scrape_HTML()
        try:
            self.year=settings.year
            self.team_htmls={}
            self.roster_htmls={}
            self.week_htmls={}

            if settings.scrape_teams==True or settings.scrape_rosters==True:
                logging.debug('Scraping loop for teams/rosters triggered\n')
                self.extract_teams()

            if settings.scrape_games==True:
                for week in range(settings.start_week,settings.end_week+1):
                    logging.info(f'Now scraping html for week {week}\n')
                    self.week_htmls[week]=[]
                    url=f'https://www.pro-football-reference.com/years/{settings.year}/week_{week}.htm'
                    logging.debug(f'Week URL: {url}')
                    week_html=self.scraper.scrape(url)
                    soup=BeautifulSoup(week_html,'html.parser')
                    week_games=soup.find_all('div',class_='game_summaries')
                    if len(week_games)==2:
                        week_games=week_games[1]
                    else:
                        week_games=week_games[0]

                    games=week_games.find_all('div',class_='game_summary expanded nohover')

                    games_count=len(games)

                    for i, game in enumerate(games):
                        logging.info(f'Scraping game {i} of {games_count}\n')
                        game_link=game.find('td',class_='right gamelink')
                        link=game_link.find('a')['href']
                        url=f'https://www.pro-football-reference.com{link}'
                        html=self.scraper.scrape(url)
                        self.week_htmls[week].append(html)
            self.save_html_dicts()
        finally:
            self.scraper.quit()
            logging.info('Scraper quit.\n')

    def save_html_dicts(self, base_path="full_week_htmls_all/"):
        import os

        if base_path and not base_path.endswith("/"):
            base_path += "/"

        os.makedirs(base_path, exist_ok=True)

        with open(f"{base_path}team_htmls.txt", "w", encoding="utf-8") as f:
            for key, value in self.team_htmls.items():
                f.write(f"{key}:\n{value}\n\n")

        with open(f"{base_path}roster_htmls.txt", "w", encoding="utf-8") as f:
            for key, value in self.roster_htmls.items():
                f.write(f"{key}:\n{value}\n\n")

        with open(f"{base_path}week_htmls.txt", "w", encoding="utf-8") as f:
            for key, value in self.week_htmls.items():
                f.write(f"{key}:\n{value}\n\n")

    
    def extract_teams(self):
        for team in teams:
            logging.info(f'Scraping {team}...\n')
            dicref=teams[team]
            base_url=f'https://www.pro-football-reference.com/teams/{dicref['url']}/'
            team_abbr=dicref['abbr']
            if self.settings.scrape_teams==True:
                logging.debug('Extracting team details')
                url=base_url+f'{self.year}_roster.htm'
                roster_html=self.scraper.scrape(url)
                self.roster_htmls[team_abbr]=roster_html
            if self.settings.scrape_rosters==True:
                logging.debug('Extracting roster details...')
                url=base_url+f'{self.year}.htm'
                team_html=self.scraper.scrape(url)
                self.team_htmls[team_abbr]=team_html
            logging.debug('Finished\n')

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
    logging.info('Initializing pipeline...\n')
    settings=default_pipeline_settings
    settings.year=year
    htmls=HTML_Layer(settings)
    obj=Season(htmls,settings)
    return

class Season(Season_Mixins):
    def __init__(self,htmls,settings):
        self.settings = settings
        self.htmls = htmls
        logging.info(f'Starting process for the {settings.year} NFL Season.\n\n')

        if settings.end_week>18:
            logging.debug('End week cannot be greater than 18- setting to 18.')
            settings.end_week=18

        settings.end_week+=1 #ensures users can specify their actual desired endweek. no need to understand how the Range loop works


        start_week=settings.start_week
        end_week=settings.end_week

        if settings.scrape_rosters is True:
            logging.debug('Extracting player tables...')
            Players=DIM_Players(settings.year,htmls)
            self.teamref=Players.df

        week_objs=[]

        if settings.scrape_teams is True:
            for team in teams:
                Team(team,htmls)

        fact_stats_dfs=[]
        fact_scores_dfs=[]
        dim_games_dfs=[]
        dim_score_details_dfs=[]

        for week in range(start_week,end_week):
            logging.info(f'Starting week {week}...')
            try:
                week_htmls=self.htmls.week_htmls[week]
            except KeyError:
                week_htmls=self.htmls.week_htmls[str(week)]
            
            if week==1:
                last_week=None
            else:
                last_week=week_objs[week-2]
            week_obj=Week(week,settings.year,week_htmls,self.teamref,last_week)
            week_objs.append(week_obj)
            fact_stats_dfs.append(week_obj.fact_stats)
            fact_scores_dfs.append(week_obj.scoring_df)
            dim_games_dfs.append(week_obj.games_df)
            dim_score_details_dfs.append(week_obj.score_details_df)
            
        self.teamref.drop(columns=['Team'],inplace=True)
        self.teamref=self.teamref.drop_duplicates(subset=['Player_ID'])

        fact_stats=pd.concat(fact_stats_dfs)
        fact_scoring=pd.concat(fact_scores_dfs)
        dim_games=pd.concat(dim_games_dfs)
        dim_score_details=pd.concat(dim_score_details_dfs)
        
        with pd.ExcelWriter('C:\\Users\\19495\\OneDrive\\Documents\\Python\\SalarySmartNFL\\NFL_Test_Webscraping_Off.xlsx',mode='a',if_sheet_exists='replace') as writer:
            fact_stats.to_excel(writer,sheet_name='FACT_Stats',index=False)
            fact_scoring.to_excel(writer,sheet_name='FACT_Scoring',index=False)
            dim_games.to_excel(writer,sheet_name='DIM_Games',index=False)
            dim_score_details.to_excel(writer,sheet_name='DIM_Score_Details',index=False)
            self.teamref.to_excel(writer,sheet_name='DIM_Players',index=False)

class Week:
    def __init__(self,week,year,htmls,roster_table,last_week):
        if len(str(week))==1:
            week=f'0{week}'
        self.week_id=f'{week}{year}'
        self.dfs={
            'fact':{
                'stats':[],
                'scoring':[]
            },
            'dimension':{
                'games':[],
                'score_details':[]
            }
        }
        for i,html in enumerate(htmls,start=1):
            game_obj=Game(self.week_id,i,html,roster_table,week,year)
            self.dfs['fact']['scoring'].append(game_obj.scoring.fact_df)
            self.dfs['fact']['stats'].append(game_obj.stats.df)
            self.dfs['dimension']['games'].append(game_obj.game.df)
            self.dfs['dimension']['score_details'].append(game_obj.scoring.dimension_df)

        self.scoring_df=pd.concat(self.dfs['fact']['scoring'])
        self.score_details_df=pd.concat(self.dfs['dimension']['score_details'])

        games_df=pd.concat(self.dfs['dimension']['games'])
        stats_df=pd.concat(self.dfs['fact']['stats'])
        week_row = pd.DataFrame([{
            "Team_ID": self.week_id,
            "Game": "Week_Summary",
            'Week': week,
            'Year': year
        }])
        week_row = pd.DataFrame(week_row)
        self.games_df = pd.concat([games_df, week_row], ignore_index=True)

        if last_week is None:
            self.season_sum = stats_df.copy()
            self.season_sum['Game_ID']=self.week_id

        else:
            self.sum_season_stats([last_week.season_sum,stats_df])
        
        self.fact_stats=pd.concat([self.season_sum,stats_df])
        
    def sum_season_stats(self,df_list):
        merged_dfs=[]
        for cat in Stat_Cat.registry:
                dfs=[]
                for df in df_list:
                    filtered_df = df[df['Stat'].str.startswith(cat.identifier)]
                    filtered_df = filtered_df[filtered_df['Stat'].isin(cat.summary_stats)]
                    filtered_df=filtered_df.pivot(index=['Player','Tm'],columns=['Stat'],values='Value')
                    filtered_df.columns.name=None
                    filtered_df = filtered_df.reset_index()      
                    dfs.append(filtered_df)
                merged = dfs[0].merge(dfs[1], on='Player', how='outer')
                merged = merged.fillna(0)
    
                for calc in cat.season_calcs:
                    if calc=='sum':
                        form=lambda x,y:x+y
                    if calc=='avg':
                        form=lambda a,b,x,y:(a+x)/(b+y)
                    if calc=='pct':
                        form=lambda a,b,x,y:((a+x)*100)/(b+y)
                    if calc=='rat':
                        form=lambda x,y:(x+y)/2
                    for col in cat.season_calcs[calc]:
                        if calc=='sum':
                            merged[f'{col}_z']=form(merged[f'{col}_x'],merged[f'{col}_y'])
                        elif calc=='rat':
                            merged[f'{col}_z']=form(merged[f'{col}_x'],merged[f'{col}_y'])
                        else:
                            cols=cat.season_calcs[calc][col]
                            merged[f'{col}_z']=form(merged[f'{cols[0]}_x'],merged[f'{cols[0]}_y'],merged[f'{cols[1]}_x'],merged[f'{cols[1]}_y'])
                zcols = merged.filter(regex='_z$').columns
                merged_z = merged[['Player','Tm_y'] + list(zcols)]
                filtered_=merged_z
                filtered_.columns = [filtered_.columns[0]] + [c[:-2] for c in filtered_.columns[1:]]
                long=pd.melt(filtered_,id_vars=['Player','Tm'],value_name='Value',var_name='Stat')
                merged_dfs.append(long)
        self.season_sum=pd.concat(merged_dfs)
        self.season_sum.fillna(0)
        self.season_sum['Game_ID']=self.week_id

# functions

class Game:
    def __init__(self,week_id,index,html,roster_table,week,year):
        soup=BeautifulSoup(html,'html.parser')
        if len(str(index))==1:
            index=f'0{index}'
        self.game_id=f'{index}{week_id}'
        self.scoring=Scoring_Tables(soup,self.game_id,roster_table)
        self.game=DIM_Games(soup,self.game_id,week,year)
        self.stats=Fact_Stats(self.game_id,soup,roster_table,self.game.df)

class DIM_Games(Season_Mixins):
    def __init__(self,soup,game_id,week,year):
        self.soup=soup

        scorebox=soup.find('div',class_='scorebox')
        self.sects=scorebox.find_all('strong')

        away_team_box=self.sects[0]
        away_team=away_team_box.get_text().strip()

        home_team_box=self.sects[2]
        home_team=home_team_box.get_text().strip()



        self.home_team_key= teams[home_team]['abbr'].upper()
        self.away_team_key=teams[away_team]['abbr'].upper()

        self.team_tags={
            self.home_team_key:f'{game_id}H',
            self.away_team_key:f'{game_id}A'
        }

        game_details_area=soup.find('div',class_='scorebox_meta')
        game_details_list=game_details_area.find_all('div')

        self.extract_from_html_list(game_details_list,Game_Details)

        game_info_box=soup.find('table',id='game_info')
        
        rows = game_info_box.find_all('td', attrs={'class': 'center', 'data-stat': 'stat'})

        self.extract_from_html_list(rows,Other_Game_Details)

        reftable=soup.find('table',id='officials')
        rows=reftable.find_all('td')
        self.extract_from_html_list(rows,ref_table_targets)

        game_desc=f'{self.home_team_key} v {self.away_team_key}'

        base_list=[game_desc,week,year,self.game_date,self.game_time,self.stadium,self.roof,self.surface,self.ref]

        home_row=[self.team_tags[self.home_team_key],self.home_team_key,self.away_team_key]+base_list
        away_row=[self.team_tags[self.away_team_key],self.away_team_key,self.home_team_key]+base_list

        rows=[home_row,away_row]

        self.df=pd.DataFrame(rows,columns=['Team_ID','Team','Opponent','Game','Week','Year','Date','Time','Stadium','Roof','Surface','Referee'])

class Fact_Stats: # orchestration
    def __init__(self,game_id,soup,roster_table,game_table):
        logging.info('Extracting fact table data...')
        
        dataframes=[]

        for cat_cls in Stat_Cat.registry:
            if cat_cls.cat=='defense':
                instance=Defense_Table(soup,cat_cls)
            else:
                instance=Stat_Table(soup,cat_cls,roster_table)
            dataframes.append(instance.df)
        self.df=pd.concat(dataframes)
        self.Add_Game_IDs(game_table)
    
    def Add_Game_IDs(self,game):
        self.df['Game_ID'] = self.df['Tm'].map(game.set_index('Team')['Team_ID'])
        self.df = self.df[['Player','Game_ID','Tm','Stat','Value']]

class Stat_Table(Fact):
    def __init__(self,soup,category,roster_table):
        self.category=category
        logging.info(f'Extracting {category.cat} data...')
        for k,v in category.__dict__.items():
            if not k.startswith('__'):
                setattr(self,k,v)
        try:
            super().__init__(category,soup)
        except MissingCols:
            raise MissingCols

        self.df=self.df[self.df['Player']!='Player'].fillna(0).infer_objects(copy=False)
        if hasattr(self, "cleaning"):
            self.clean_table()
        self.typecheck()
        self.calculate_values()
        self.long_now()
        self.sub_ids(roster_table.copy())

    def sub_ids(self,roster_table):
        self.sub_player_ids(roster_table)
        self.sub_stat_ids()

    def sub_player_ids(self,roster_table):
        roster_table['merge_key'] = roster_table['Name'] + "_" + roster_table['Team']
        self.df['merge_key'] = self.df['Player'] + "_" + self.df['Tm']
        id_map = roster_table.set_index('merge_key')['Player_ID']
        self.df['Player'] = self.df['merge_key'].map(id_map)

        unmapped = self.df[self.df['Player'].isna()]
        if not unmapped.empty:
            logging.warning(f"Players not found in roster: {unmapped['merge_key'].unique()}")

        self.df.drop(columns=['merge_key'], inplace=True)


    def sub_stat_ids(self):
        mapping_dict = dim_stats[self.category.cat]
        self.df['Stat'] = self.df['Stat'].map(mapping_dict)

class Scoring_Tables(Fact):
    def __init__(self,soup,game_id,roster_table):
        global teams_df
        category=Scoring
        self.game_id=game_id
        for k,v in category.__dict__.items():
            if not k.startswith('__'):
                setattr(self,k,v)
        super().__init__(category,soup)
        teams = teams_df.rename(columns={'index': 'Name'})
        self.df = self.df.merge(
            teams[['mascot','abbr','location','url']],
            left_on='Tm',
            right_on='mascot',
            how='left'
        )
        self.df['Tm'] = self.df['abbr']
        self.df = self.df.drop(columns=['mascot','abbr','location','url'])
        self.df = self.df.iloc[:, :-2]
        self.quarter=1
        self.df_rows=[]
        self.dim_rows=[]
        self.details={}
        for i,row in enumerate(self.df.iterrows()):
            self.score_id=f's{i}{self.game_id}'
            row=row[1]
            self.set_quarter(row['Quarter'])
            dimension_row=[self.score_id,self.quarter,row['Tm'],self.game_id]
            self.dim_rows.append(dimension_row)
            self.details[self.score_id]=row['Detail']
        self.fact=Fact_Scoring(self.details)
        self.generate_dimension()
        merge_df=pd.merge(left=self.fact.df,right=self.dimension_df,how='left',on='Score_ID')
        merged = merge_df.merge(
            roster_table[['Name','Team','Player']],
            left_on=['Scorer','Team'],
            right_on=['Name','Team'],
            how='left'
        )

        merged = merged.drop(columns=['Scorer','Name','Team','Quarter']).rename(columns={'Player':'Scorer'})
        merged = merged[['Score_ID','Scorer','Game ID','Detail','value']]
        self.fact_df=merged

    def generate_dimension(self):
        self.dimension_df=pd.DataFrame(self.dim_rows,columns=['Score_ID','Quarter','Team','Game ID'])

    def set_quarter(self,quarter):
        try:
            if int(self.quarter)>int(quarter):
                return
        except ValueError: #occurs when the game goes into overtime, resulting in the quarter being marked as "OT"
            if quarter=='OT':
                quarter=5
                self.set_quarter(quarter)
        self.quarter=quarter
        
class Fact_Scoring(Fact):
    def __init__(self,details):
        dfs=[]
        for score in details:
            df=self.parse_details(details[score])
            df['Score_ID']=score
            dfs.append(df)
        df=pd.concat(dfs)
        Elphaba=pd.melt(df,id_vars=['Score_ID','Scorer'],var_name='Detail')
        self.df=Elphaba[Elphaba['value'].notna()]
        
    def parse_details(self,details):
        scorer, distance, other=self.parse_score(details)
        if scorer==None:
            df=self.parse_special(details)
            return df
        method=self.play_type(other)
        if method=='pass':
            passer=self.get_passer(other)
        else:
            passer = pd.NA
        if method!='field goal':
            type='TD'
            if '(' not in other:
                df=pd.DataFrame([[scorer,passer,type,distance,method]],columns=['Scorer','Passer','Type','Distance','Method'])
                return df
            detail=self.get_parenthetical(other)
            dets=[[scorer,passer,type,distance,method]]
            df1=pd.DataFrame(dets,columns=['Scorer','Passer','Type','Distance','Method'])
            df2=self.get_extra_point(detail)
            df=pd.concat([df1, df2], axis=0)
        else:
            type='FG'
            dets=[[scorer,type,distance]]
            df=pd.DataFrame(dets,columns=['Scorer','Type','Distance'])
        return df
    
    def parse_special(self,details):
        if 'Safety' in details:
            type='Safety'
            df=pd.DataFrame([[type]],columns=['Type'])
            return df

    def get_extra_point(self,detail):
        type='XP'
        if 'kick' in detail:
            method='kick'
        elif 'run' in detail:
            method='run'
        else:
            method='pass'
        if 'failed' in detail:
            good=False
            df=pd.DataFrame([[type,good,method]],columns=['Type','Good','Method'])
            return df
        else:
            good=True
        desc=detail.replace(method,'').strip()
        if method!='pass':
            scorer=desc
            df=pd.DataFrame([[type,good,method,scorer]],columns=['Type','Good','Method','Scorer'])
            return df
        else:
            parts=re.split(r'from',desc,1)
            left=parts[0].strip()
            right=parts[1].strip()
            scorer=left.strip()
            passer=right.strip()
            df=pd.DataFrame([[type,good,method,scorer,passer]],columns=['Type','Good','Method','Scorer','Passer'])
            return df

    def get_parenthetical(self,s):
        m=re.search(r'\((.*?)\)', s)
        if m:
            detail=m.group(1).strip()
            return detail
        return None

    def get_passer(self,s):
        m=re.search(r'pass from\s*([A-Za-z .\'-]+?)(?=\(|$)', s, flags=re.IGNORECASE)
        if m:
            return m.group(1).strip()
        return None

    def play_type(self,s):
        s=s.lower()
        if 'field goal' in s:
            return 'field goal'
        elif 'pass' in s:
            return 'pass'
        elif 'rush' in s:
            return 'rush'
        elif 'kickoff return' in s:
            return 'kickoff return'
        elif 'blocked punt return' in s:
            return 'blocked punt return'
        elif 'punt return' in s:
            return 'punt return'
        elif 'interception return':
            return 'interception return'
        else:
            return 'unidentified'

    def parse_score(self,text):
        m=re.search(r'^(.*?)(\d+)\s+yard\s+(.*)$',text)
        if not m:
            return None,None,None
        left=m.group(1).strip()
        num=int(m.group(2))
        right=m.group(3)
        return left,num,right

class Scoring(HTML_Extraction):
    id='scoring'
    expected_cols={'Quarter':object,'Time':object,'Detail':object}
    cat='scoring'
    quarter=1
    time=2
    team=3
    detail=4

# constants

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
        'sum':['P1','P2','P3','P6','P8','P10','P13','P15','P17','P19','P20','P21','P22','P23','P28'],
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
    
    season_vals=['P1','P2','P3','P4','P5','P6','P7','P8','P9','P10','P11','P12','P13','P14','P15','P16','P17',
                 'P18','P19','P20','P21','P22','P23','P28']

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

class Other_Game_Details:
    roof=1
    surface=2

# Fact Table functionality
        
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
            self.df=table.df.copy()
            self.generate_player_id(self.df['Player'],self.df['BirthDate'])
            self.df['Team']=teams[team]['abbr']
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

# helpers

class Scraper_Settings:
    def __init__(self,rosters,teams,games,start_week,end_week):
        self.scrape_rosters=rosters
        self.scrape_teams=teams
        self.scrape_games=games
        self.start_week=start_week
        self.end_week=end_week
