from neo4j import GraphDatabase
import pandas as pd
from typing import List, Dict, Any
import sys
import os

# Add parent directory to path for queries import
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from queries import CypherQueries


class Neo4jClient:
    """Neo4j graph database client"""
    
    def __init__(self, uri, user, password):
        """
        Args:
            uri (str): Neo4j connection URI
            user (str): Username
            password (str): Password
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def get_all_nodes(self) -> List[str]:
        """
        Get all node names from the database
        """
        def _get_nodes(tx):
            query = CypherQueries.get_all_nodes()
            result = tx.run(query)
            return [record["n"]['name'] for record in result]
        
        with self.driver.session() as session:
            return session.read_transaction(_get_nodes)
    
    def get_food_nodes(self) -> List[str]:
        """
        Get all Food node names
        """
        def _get_food_nodes(tx):
            query = CypherQueries.get_all_food_nodes()
            result = tx.run(query)
            return [record["f"]['name'] for record in result]
        
        with self.driver.session() as session:
            return session.read_transaction(_get_food_nodes)
    
    def get_food_nodes_without_metadata(self) -> pd.DataFrame:
        """
        Get Food nodes that don't have metadata
        """
        with self.driver.session() as session:
            query = CypherQueries.get_food_nodes_without_metadata()
            result = session.run(query)
            
            data = []
            for record in result:
                node_properties = record["node_properties"]
                # Exclude embedding property
                filtered_properties = {k: v for k, v in node_properties.items() if k != 'embedding'}
                data.append(filtered_properties)
            
            return pd.DataFrame(data)
    
    def insert_node(self, name: str, label: str, properties: Dict[str, Any] = None):
        """
        Insert a single node
        
        Args:
            name (str): Node name
            label (str): Node label (Food, parent_node, etc)
            properties (dict): Additional properties
        """
        with self.driver.session() as session:
            query = CypherQueries.merge_node(label)
            if properties:
                set_clause = ", ".join([f"n.{k} = ${k}" for k in properties.keys()])
                query += f" SET {set_clause}"
            
            params = {'name': name}
            if properties:
                params.update(properties)
            
            session.run(query, **params)
    
    def create_relationship(self, from_node: str, to_node: str, rel_type: str):
        """
        Create a relationship between two nodes
        
        Args:
            from_node (str): Source node name
            to_node (str): Target node name
            rel_type (str): Relationship type (contain, characterize, inherit, same)
        """
        query = CypherQueries.create_relationship(rel_type)
        with self.driver.session() as session:
            session.run(query, from_node=from_node, to_node=to_node)
    
    def delete_all_relationships(self, rel_type: str = None):
        """
        Delete all relationships or relationships of a specific type
        
        Args:
            rel_type (str, optional): Relationship type to delete. If None, deletes all.
        """
        if rel_type:
            query = CypherQueries.delete_relationships_by_type(rel_type)
        else:
            query = CypherQueries.delete_all_relationships()
        
        with self.driver.session() as session:
            session.run(query)
    
    def execute_query(self, query: str, parameters: Dict[str, Any] = None):
        """
        Execute a custom Cypher query
        """
        with self.driver.session() as session:
            return session.run(query, parameters or {})
