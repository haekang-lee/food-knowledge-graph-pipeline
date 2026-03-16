import pymysql
import pandas as pd


class MySQLClient:
    """MySQL database client"""
    
    def __init__(self, conn_info):
        
        self.conn_info = conn_info
    
    def fetch_data(self, query):
        
        connection = pymysql.connect(
            host=self.conn_info['host'],
            user=self.conn_info['user'],
            password=self.conn_info['password'],
            database=self.conn_info['database'],
            cursorclass=pymysql.cursors.DictCursor
        )
        
        try:
            with connection.cursor() as cursor:
                cursor.execute(query)
                result = cursor.fetchall()
            connection.commit()
        finally:
            connection.close()
        
        return pd.DataFrame(result)
