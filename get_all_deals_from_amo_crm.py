#get_all_deals_from_amo_crm
#Забираем из AmoCRM все сделки
#Складываем их на сервере в AMO DEALS JSON WEEK
#Загружаем в BigQuery
#
import AmoCRM
import GoogleBQ_deals
import config
import os
from loguru import logger
from heapq import merge


logger.add("info.log", format="{time} {level} {module} : {function} - {message}", level="INFO", rotation="10:00", compression="zip")
logger.add("connection_errors.log", format="{time} {level} {module} : {function} - {message}", level="ERROR", rotation="10:00", compression="zip")
logger.level('df_errors.log', no=14, color='<red>', icon='❌❌')
logger.level('connection_errors', no=15, color='<red>', icon='❌❌❌')

logger.info('Запускаю скрипт update_deals_from_amo_crm.py')
amo_robot = AmoCRM.AmoCRM()
bq_robot = GoogleBQ_deals.GoogleBQ_deals()
head = bq_robot.get_deals_table_head()
pipelines_list = [7038, 28752] #идентификаторы воронок, из которых хотим забирать сделки


#Скачиваем все сделки из AmoCRM в JSON файлы и разбиваем их по неделям
amo_robot.get_all_deals_from_crm()

logger.info('Все сделки из AmoCRM успешно выгружены на сервер и добавлены в AMO DEALS WEEK JSON')

logger.info('Загружаем сделки в BigQuery')
#получаем путь к Amo Deals Week Json
week_json_path = amo_robot.get_deals_week_json_path()
#получаем список файлов:
files_list = os.listdir(week_json_path)
for filename in files_list:
    #получаем датафрейм из json-файла
    df_deals = amo_robot.get_deals_dataframe_from_file(week_json_path, filename, head, pipelines_list)
    #мерджим датафрейм в BigQuery
    bq_robot.merge_deals_dataframe_to_bigquery(df_deals)
    logger.info(f'файл {filename} успешно загружен в BigQuery')

#put_deals_from_amo_week_json_to_bigquery()
logger.info('Все сделки успешно загружены в BigQuery')








### Доделки
### Логгировать количество выгруженных из AmoCRM сделок
### Вынести в этот модуль указание куда складывать файлы

