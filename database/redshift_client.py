import psycopg2
import pandas as pd


class RedshiftClient:
    """Redshift database client"""
    
    def __init__(self, conn_info):
        
        self.conn_info = conn_info
    
    def fetch_data(self, query):
        
        conn = psycopg2.connect(
            host=self.conn_info['host'],
            port=self.conn_info['port'],
            dbname=self.conn_info['database'],
            user=self.conn_info['user'],
            password=self.conn_info['password']
        )
        
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        
        # Extract column names
        column_names = [desc[0] for desc in cur.description]
        
        cur.close()
        conn.close()
        
        return pd.DataFrame(rows, columns=column_names)
