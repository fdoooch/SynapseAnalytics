#google_sheets.py
#from google.oauth2.service_account import Credentials
#from googleapiclient.discovery import build
import gspread
from oauth2client.service_account import ServiceAccountCredentials

def google_one():

    scope = ["https://spreadsheets.google.com/feeds",'https://www.googleapis.com/auth/spreadsheets',"https://www.googleapis.com/auth/drive.file","https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name('api_keys/google_api_creds.json', scope)
    client = gspread.authorize(creds)
    sheet = client.open_by_url('https://docs.google.com/spreadsheets/d/1DIFKSRgzLprxlq6SYbeHYFPp9RqhoRw_qRAkTZl-fEY').sheet1
    data = sheet.get_all_records()
    print(data)

