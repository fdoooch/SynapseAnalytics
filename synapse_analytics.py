#synapse_analytics
import os
import config
import amo_crm as amo
from loguru import logger

logger.add("debug.log", format="{time} {level} {module} : {function} - {message}", level="DEBUG", rotation="10:00", compression="zip")
print('hello Synapse Analytics!')


amo.amo_get_all_deals_ext_to_json()