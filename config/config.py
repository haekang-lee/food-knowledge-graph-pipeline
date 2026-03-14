import os
from dotenv import load_dotenv

load_dotenv()


class DatabaseConfig:
    
    # MySQL
    KIDS_HOST = os.getenv('KIDS_HOST')
    KIDS_USER = os.getenv('KIDS_USER')
    KIDS_PASSWORD = os.getenv('KIDS_PASSWORD')
    KIDS_DATABASE = os.getenv('KIDS_DATABASE')
    
    # Redshift
    REDSHIFT_HOST = os.getenv('REDSHIFT_HOST')
    REDSHIFT_USER = os.getenv('REDSHIFT_USER')
    REDSHIFT_PASSWORD = os.getenv('REDSHIFT_PASSWORD')
    REDSHIFT_DATABASE = os.getenv('REDSHIFT_DATABASE')
    REDSHIFT_PORT = os.getenv('REDSHIFT_PORT')
    
    # Neo4j
    NEO4J_URI = os.getenv('NEO4J_URI')
    NEO4J_USER = os.getenv('NEO4J_USER')
    NEO4J_PASSWORD = os.getenv('NEO4J_PASSWORD')
    
    @classmethod
    def get_kids_conn_info(cls):
        return {
            'host': cls.KIDS_HOST,
            'user': cls.KIDS_USER,
            'password': cls.KIDS_PASSWORD,
            'database': cls.KIDS_DATABASE
        }
    
    @classmethod
    def get_redshift_conn_info(cls):
        return {
            'host': cls.REDSHIFT_HOST,
            'user': cls.REDSHIFT_USER,
            'password': cls.REDSHIFT_PASSWORD,
            'database': cls.REDSHIFT_DATABASE,
            'port': cls.REDSHIFT_PORT
        }


class APIConfig:
    
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
    
    @classmethod
    def load_openai_key_from_file(cls, filepath='../config/openai_key.txt'):
        
        try:
            # Get the directory of this config file
            current_dir = os.path.dirname(os.path.abspath(__file__))
            # Go up to src, then up to GraphDB, then into config
            base_dir = os.path.dirname(os.path.dirname(current_dir))
            full_path = os.path.join(base_dir, 'config', 'openai_key.txt')
            
            with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                return f.read().strip()
        except FileNotFoundError:
            return cls.OPENAI_API_KEY


class GoogleSheetsConfig:
    # Google Sheets API configurations
    
    CREDENTIALS_PATH = os.getenv('GOOGLE_CREDENTIALS_PATH', '../config/google_credentials.json')
    SPREADSHEET_URL = os.getenv('GOOGLE_SPREADSHEET_URL')
    
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    
    @classmethod
    def get_credentials_path(cls):
        # Get absolute path to credentials file
        if os.path.isabs(cls.CREDENTIALS_PATH):
            return cls.CREDENTIALS_PATH
        
        # Get relative path from src directory
        current_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.dirname(os.path.dirname(current_dir))
        return os.path.join(base_dir, 'config', 'google_credentials.json')