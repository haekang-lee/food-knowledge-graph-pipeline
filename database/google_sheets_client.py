import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd


class GoogleSheetsClient:
    """Google Sheets API client"""
    
    def __init__(self, credentials_path, spreadsheet_url, scopes=None):
        """
        Args:
            credentials_path (str): Path to service account credentials JSON file
            spreadsheet_url (str): Google Sheets URL
            scopes (list, optional): API scopes
        """
        self.credentials_path = credentials_path
        self.spreadsheet_url = spreadsheet_url
        self.scopes = scopes or [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        self.client = None
        self.spreadsheet = None
        self._authenticate()
    
    def _authenticate(self):
        """Authenticate with Google Sheets API"""
        creds = Credentials.from_service_account_file(
            self.credentials_path, 
            scopes=self.scopes
        )
        self.client = gspread.authorize(creds)
        self.spreadsheet = self.client.open_by_url(self.spreadsheet_url)
    
    def read_sheet(self, sheet_name: str) -> pd.DataFrame:
        """
        Args:
            sheet_name (str): Name of the sheet to read
        """
        sheet = self.spreadsheet.worksheet(sheet_name)
        data = sheet.get_all_records()
        return pd.DataFrame(data)
    
    def write_sheet(self, df: pd.DataFrame, sheet_name: str, append: bool = False):
        # Write DataFrame to a sheet
        creds = ServiceAccountCredentials.from_json_keyfile_name(
            self.credentials_path, 
            self.scopes
        )
        client = gspread.authorize(creds)
        spreadsheet = client.open_by_url(self.spreadsheet_url)
        
        try:
            worksheet = spreadsheet.worksheet(sheet_name)
        except gspread.exceptions.WorksheetNotFound:
            worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="100", cols="20")
            next_row = 1
        else:
            if append:
                existing_values = worksheet.get_all_values()
                next_row = len(existing_values) + 1
            else:
                next_row = 1
        
        set_with_dataframe(
            worksheet, 
            df, 
            row=next_row, 
            include_column_header=(next_row == 1)
        )
