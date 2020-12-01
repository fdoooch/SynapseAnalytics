#get_all_deals_from_amo_crm
#Забираем из AmoCRM все сделки
#Складываем их на сервере в AMO DEALS JSON WEEK
#Загружаем в BigQuery
#
import config
import os
import amo_crm as amo
import google_bigquery as gb
import json
from loguru import logger
from heapq import merge

#Добавляем к датафрейму дополнительные поля для упрощения поиска в BQ
def add_bq_special_fields_to_amo_deals_dataframe(df_amo_deals):
    add_bq_special_fields_to_amo_deals_dataframe(df_amo_deals)
    return 0


#Загружаем сделки в BigQuery
def put_deals_from_amo_week_json_to_bigquery():
    week_json_path = config.AMO_LEADS_WEEK_JSON_PATH
    #создаём таблицу, если её нет
    with open(config.BQ_TB_AMO_DEALS_RAW_SHEMA, 'r', encoding="utf8") as json_schema:
        tb_schema = json.load(json_schema)
    gb.create_table(config.BQ_DS_MAIN, config.BQ_TB_AMO_DEALS_RAW, tb_schema)
    #Получаем список json week файлов
    files_list = os.listdir(week_json_path)
    #Обрабатываем файлы
    for filename in files_list:    
        with open(week_json_path + filename, 'r', encoding="utf8") as json_file:
            #Загружаем json в датафрейм
            json_deals = json.load(json_file)
            df_deals = amo.get_dataframe_from_json_deals(json_deals)
            df_deals = add_bq_special_fields_to_amo_deals_dataframe(df_deals)
            logger.info(f'Файл {filename} загружен в датафрейм.')
            #Мерджим датафрейм в BigQuery
            amo_deal_fields_list = list(merge(config.AMO_DEALS_BASE_FIELDS, config.BQ_DEALS_SPECIAL_FIELDS, set(config.AMO_DEALS_SPECIAL_FIELDS.keys), set(config.AMO_DEALS_CUSTOM_FIELDS.keys())))
            gb.merge_bq_table_with_dataframe(df_deals, config.BQ_DS_MAIN, config.BQ_TB_AMO_DEALS_RAW, tb_schema, amo_deal_fields_list, 'id')
            logger.info(f'Файл {filename} добавлен в BigQuery.')

logger.add("info.log", format="{time} {level} {module} : {function} - {message}", level="INFO", rotation="10:00", compression="zip")

logger.info('Загружаем все сделки из AmoCRM на сервер')
#amo.amo_get_token_fdoooch()
#amo.amo_get_all_deals_ext_to_json(config.AMO_LEADS_RAW_JSON_PATH, 
 #                                 config.AMO_PAGE_SIZE, 
 #                                 config.AMO_PAGES_COUNT_PER_LOAD, 
 #                                 config.AMO_ALL_LEADS_EXT_JSON_FILENAME)
logger.info('Все сделки из AmoCRM успешно выгружены на сервер')
logger.info('Добавляем сделки в AMO LEADS WEEK JSON')
#amo.amo_put_deals_from_raw_json_to_week_json(config.AMO_LEADS_RAW_JSON_PATH, config.AMO_LEADS_WEEK_JSON_PATH)
logger.info('сделки добавлены в AMO LEADS WEEK JSON')
logger.info('Загружаем сделки в BigQuery')
put_deals_from_amo_week_json_to_bigquery()
logger.info('Все сделки успешно загружены в BigQuery')








### Доделки
### Логгировать количество выгруженных из AmoCRM сделок
### Вынести в этот модуль указание куда складывать файлы

