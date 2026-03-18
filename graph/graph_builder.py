import uuid
import pandas as pd
from tqdm import tqdm
from typing import List, Tuple
from langchain_openai import OpenAIEmbeddings
from ..database import Neo4jClient
from ..queries import CypherQueries


class GraphBuilder:
    """Builds and manages Neo4j graph database"""
    
    def __init__(self, neo4j_client: Neo4jClient, openai_api_key: str):
        """
        Args:
            neo4j_client: Neo4j database client
            openai_api_key: OpenAI API key for embeddings
        """
        self.neo4j_client = neo4j_client
        self.embeddings = OpenAIEmbeddings(
            model="text-embedding-3-small", # 임베딩 모델
            openai_api_key=openai_api_key
        )
    
    def generate_embeddings(self, menu_items: List[str]) -> List[List[float]]:
        """
        Generate embeddings for menu items
        
        Args:
            menu_items (list): List of menu names
        """
        return self.embeddings.embed_documents(menu_items)
    
    def insert_food_nodes(self, df: pd.DataFrame, embeddings_list: List[List[float]]):
        """
        Insert Food nodes with embeddings
        
        Args:
            df (pd.DataFrame): DataFrame containing food nodes
            embeddings_list (list): List of embedding vectors
        """
        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Inserting Food nodes"):
            food_name = row['menu']
            embedding = embeddings_list[index]
            node_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, food_name))
            
            query = CypherQueries.merge_food_node_with_embedding()
            
            with self.neo4j_client.driver.session() as session:
                session.run(query, food_name=food_name, embedding=embedding, node_id=node_id)
        
        print("Food 노드 INSERT 완료!")
    
    def insert_nodes_without_embedding(self, df: pd.DataFrame, label_name: str):
        """
        Insert nodes without embeddings (parent_node, etc)
        
        Args:
            df (pd.DataFrame): DataFrame containing nodes
            label_name (str): Node label (parent_node or etc)
        """
        if df.empty:
            print(f"No {label_name} nodes to insert")
            return
        
        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc=f"Inserting {label_name} nodes"):
            node_name = row['menu']
            node_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, node_name))
            
            query = CypherQueries.merge_node_with_id(label_name)
            
            self.neo4j_client.execute_query(query, {'node_name': node_name, 'node_id': node_id})
        
        print(f"{label_name} 노드 INSERT 완료!")
    
    def create_relationships(self, df: pd.DataFrame):
        """
        Create relationships between nodes
        
        Args:
            df (pd.DataFrame): DataFrame containing node relationships
        """
        for index, row in tqdm(df.iterrows(), total=df.shape[0], desc="Creating relationships"):
            # Process etc, sub1~sub4, 상위노드
            for col in ['etc', 'sub1', 'sub2', 'sub3', 'sub4', '상위노드']:
                if pd.notna(row[col]):
                    values = str(row[col]).split('/')
                    rel_type = 'characterize' if col == 'etc' else 'contain' if col.startswith('sub') else 'inherit'
                    
                    for value in values:
                        self.neo4j_client.create_relationship(row['menu'], value, rel_type)
    
    def create_same_relationships(self, synonym_list: List[List[Tuple[str, str]]]):
        """
        Create 'same' relationships for synonyms
        
        Args:
            synonym_list (list): List of synonym pairs
        """
        for syn in tqdm(synonym_list, desc='Creating same relationships'):
            for pair in syn:
                self.neo4j_client.create_relationship(pair[0], pair[1], 'same')
        
        print("SAME 관계 생성 완료!")
    
    def delete_same_relationships(self):
        """Delete all 'same' relationships"""
        query = CypherQueries.delete_same_relationships()
        with self.neo4j_client.driver.session() as session:
            session.run(query)
        print("SAME 관계 삭제 완료!")
    
    def get_nodes_to_insert(self, df_food: pd.DataFrame, df_parent: pd.DataFrame, 
                           df_etc: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """
        Get nodes that need to be inserted (not already in database)
        
        Args:
            df_food: Food nodes dataframe
            df_parent: Parent nodes dataframe
            df_etc: Etc nodes dataframe
        """
        graphdb_node_list = self.neo4j_client.get_all_nodes()
        print(f"현재 DB에 등록된 노드 개수: {len(graphdb_node_list)}개")
        
        insert_df_food = df_food[~df_food['menu'].isin(graphdb_node_list)]
        insert_df_parent = df_parent[~df_parent['menu'].isin(graphdb_node_list)]
        insert_df_etc = df_etc[~df_etc['menu'].isin(graphdb_node_list)]
        
        print(f"새로 추가할 Food 노드: {len(insert_df_food)}개")
        print(f"새로 추가할 Parent 노드: {len(insert_df_parent)}개")
        print(f"새로 추가할 Etc 노드: {len(insert_df_etc)}개")
        
        return insert_df_food, insert_df_parent, insert_df_etc
