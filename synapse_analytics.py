#synapse_analytics
import os
import config
import amo_crm as amo
import google_sheets as gsheets
from loguru import logger
#from pprint import pprint

#logger.add("debug.log", format="{time} {level} {module} : {function} - {message}", level="DEBUG", rotation="10:00", compression="zip")
logger.add("info.log", format="{time} {level} {module} : {function} - {message}", level="INFO", rotation="10:00", compression="zip")
print('hello Synapse Analytics!')


#amo.amo_refresh_access_token()
#print('\ntoken:', os.getenv("AMO_ACCESS_TOKEN"))

#amo.amo_refresh_access_token()

#amo.amo_get_token_fdoooch()
#amo.amo_put_deals_from_raw_json_to_week_json()
df = amo.amo_get_dataframe_from_json_week('amo_json_2020_40.json')
#amo.amo_get_all_deals_ext_to_json()
#df = amo.amo_get_dataframe_from_json_week_by_year_number(2017)
print(df.shape)