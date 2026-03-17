import pandas as pd
import numpy as np
import ast
from datetime import datetime
from typing import List, Set
from ..database import MySQLClient, RedshiftClient, Neo4jClient, GoogleSheetsClient
from ..utils import clean_special_chars, encoding_list
from ..queries import MySQLQueries, RedshiftQueries


class DataCollector:
    """Collects menu data from various sources"""
    
    def __init__(self, mysql_client: MySQLClient, redshift_client: RedshiftClient, 
                 neo4j_client: Neo4jClient, sheets_client: GoogleSheetsClient):
        """
        Args:
            mysql_client: MySQL database client
            redshift_client: Redshift database client
            neo4j_client: Neo4j database client
            sheets_client: Google Sheets client
        """
        self.mysql_client = mysql_client
        self.redshift_client = redshift_client
        self.neo4j_client = neo4j_client
        self.sheets_client = sheets_client
    
    def get_existing_nodes(self) -> Set[str]:
        """
        Get existing nodes from FoodBeat (Google Sheets)
        """
        final_df = self.sheets_client.read_sheet("Node_Extraction_최종")
        
        unique_values = pd.unique(
            final_df[['menu', 'etc', 'sub1', 'sub2', 'sub3', 'sub4', '상위노드']].values.ravel('K')
        )
        
        unique_values = unique_values[~pd.isnull(unique_values)]
        unique_values = [item for item in unique_values if item != '']
        
        # Split by '/' and '='
        new_unique_values = []
        for i in unique_values:
            if '/' in i:
                new_unique_values.extend(i.split('/'))
            elif '=' in i:
                new_unique_values.extend(i.split('='))
            else:
                new_unique_values.append(i)
        
        return set(pd.unique(np.array(new_unique_values)))
    
    def get_typo_list(self) -> List[str]:
        """
        Get list of typo menus from Google Sheets
        """
        typo_df = self.sheets_client.read_sheet("BUFFER_KIDS")
        return typo_df[
            (typo_df['menu'].notnull()) & (typo_df['오탈자'] == 'TRUE')
        ]['menu'].unique().tolist()
    
    def get_menus_from_databases(self, start_date: str) -> List[str]:
        """
        Get menu names from all databases after a specific date
        
        Args:
            start_date (str): Start date in 'YYYY-MM-DD' format
        """
        # Get menus from nuvi_foods
        query_nuvifoods = MySQLQueries.get_menus_from_nuvi_foods(start_date)
        menu_list_nuvifoods = self.mysql_client.fetch_data(query_nuvifoods)
        menu_list_nuvifoods = [
            x for x in menu_list_nuvifoods['processed_name_1st'].unique() 
            if (x != '') and (x is not None)
        ]
        
        # Get menus from cluster
        query_cluster = RedshiftQueries.get_menus_from_cluster(start_date)
        menu_list_cluster = self.redshift_client.fetch_data(query_cluster)
        menu_list_cluster_lst = []
        for x in menu_list_cluster['menu_list']:
            if x:
                menu_list_cluster_lst.extend(ast.literal_eval(x))
        menu_list_cluster_lst = [
            x for x in list(set(menu_list_cluster_lst)) 
            if (x != '') and (x is not None)
        ]
        
        # Get menus from meal_plan
        query_meal_plan = MySQLQueries.get_menus_from_meal_plan(start_date)
        menu_list_meal_plan = self.mysql_client.fetch_data(query_meal_plan)
        menu_list_meal_plan = [
            x for x in menu_list_meal_plan['food_name'].unique() 
            if (x != '') and (x is not None)
        ]
        
        # Combine all menus
        food_names = list(set(menu_list_nuvifoods + menu_list_cluster_lst + menu_list_meal_plan))
        
        # Clean special characters and normalize encoding
        food_names = list(set(clean_special_chars(food_names)))
        food_names = list(set(encoding_list(food_names)))
        
        return food_names
    
    def get_new_menus(self, start_date: str) -> pd.DataFrame:
        """
        Get new menus that don't exist in FoodBeat
        
        Args:
            start_date (str): Start date in 'YYYY-MM-DD' format
        """
        # Get existing nodes
        existing_nodes = self.get_existing_nodes()
        
        # Get typo list
        typo_list = self.get_typo_list()
        
        # Get menus from databases
        food_names = self.get_menus_from_databases(start_date)
        
        # Find new menus
        new_add_nodes = list(set(food_names) - existing_nodes - set(typo_list))
        
        print(f"Period: {start_date} ~ {datetime.today().strftime('%Y-%m-%d')}")
        print(f"Total unique menus: {len(food_names)}")
        print(f"New menus to add: {len(new_add_nodes)}")
        print(f"Percentage not in FoodBeat: {round(len(new_add_nodes) / len(food_names) * 100, 2)}%")
        
        # Create DataFrame
        new_food_beat = pd.DataFrame(sorted(new_add_nodes), columns=['menu'])
        new_food_beat['etc'] = ''
        new_food_beat['sub1'] = ''
        new_food_beat['sub2'] = ''
        new_food_beat['sub3'] = ''
        new_food_beat['sub4'] = ''
        new_food_beat['상위노드'] = ''
        
        return new_food_beat
    
    def get_parent_nodes(self) -> List[str]:
        """
        Get list of parent nodes from FoodBeat
        """
        final_df = self.sheets_client.read_sheet("Node_Extraction_최종")
        final_df = final_df.replace('', np.nan)
        
        # Extract parent nodes without '/' or '='
        parent_nodes = [
            x for x in final_df['상위노드'].unique() 
            if ('/' not in str(x)) and ('=' not in str(x)) and pd.notna(x)
        ]
        
        return parent_nodes
