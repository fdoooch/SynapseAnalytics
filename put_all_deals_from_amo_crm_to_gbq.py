#put_all_deals_from_amo_crm_to_gbq.py
#Выгружаем все сделки из AmoCRM и загружаем их в Google BigQuery

import AmoCRM
import GoogleBQ_deals
import os
import config
from loguru import logger

logger.add("info.log", format="{time} {level} {module} : {function} - {message}", level="INFO", rotation="10:00", compression="zip")
logger.level('df_errors.log', no=14, color='<red>', icon='❌❌')
amo_robot = AmoCRM.AmoCRM()

logger.info('Запускаю скрипт put_all_deals_from_amo_crm_to_gbq.py')
logger.info('Выгружаю все сделки из AmoCRM в raw_json-файлы')
amo_robot.get_all_deals_from_crm()
logger.info('Все сделки выгружены')
bq_robot = GoogleBQ_deals.GoogleBQ_deals()
head = bq_robot.get_deals_table_head()
pipelines_list = [7038, 28752] #идентификаторы воронок, из которых хотим забирать сделки
for year in range(2018, 2021):
    logger.info(f'Собираю датафрейм за {year} год')
    df_deals = amo_robot.get_deals_dataframe_by_year(year, head, pipelines_list)
    #df_deals = amo_robot.get_deals_dataframe_by_week(2020, 41, head)
    bq_robot.merge_deals_dataframe_to_bigquery(df_deals)
    logger.info(f'Сделки за {year} загружены в BQ')
logger.info('все сделки загружены в Google BigQuery')