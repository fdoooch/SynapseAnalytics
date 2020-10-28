#synapse_analytics
import os
import config
import amo_crm as amo
import google_sheets as gsheets
from loguru import logger

logger.add("debug.log", format="{time} {level} {module} : {function} - {message}", level="DEBUG", rotation="10:00", compression="zip")
print('hello Synapse Analytics!')


#amo.amo_refresh_access_token()
#print('\ntoken:', os.getenv("AMO_ACCESS_TOKEN"))

amo.amo_refresh_access_token()
amo.amo_get_all_deals_ext_to_json()