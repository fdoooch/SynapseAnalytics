#config.py
import os
# settings.py
from dotenv import load_dotenv
from pathlib import Path  # Python 3.6+ only

print('config подключен')

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

GOOGLE_CLOUD_PROJECT_ID = "synapse-analytics"
API_KEYS_PATH = "api_keys/"
GOOGLE_CREDENTIALS_JSON_FILE_NAME = "google_api_creds.json"
BQ_DS_MAIN = 'ds_synapse_analytics'
BQ_DS_TEMP = 'ds_temp'
BQ_TB_AMO_DEALS_RAW = 'tb_amo_deals_raw'

BQ_TB_AMO_DEALS_RAW_SHEMA = 'bq_tb_amo_deals_raw_shema.json'