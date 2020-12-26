
#update_deals_from_amo_crm.py
#Выгружаем сделки из AmoCRM, обновлённые с даты последнего обновления в Google BigQuery и загружаем их в Google BigQuery

import AmoCRM
import GoogleBQ_deals
import os
import config
from loguru import logger

logger.add("info.log", format="{time} {level} {module} : {function} - {message}", level="INFO", rotation="10:00", compression="zip")
logger.add("connection_errors.log", format="{time} {level} {module} : {function} - {message}", level="ERROR", rotation="10:00", compression="zip")
logger.level('df_errors.log', no=14, color='<red>', icon='❌❌')
logger.level('connection_errors', no=15, color='<red>', icon='❌❌❌')

logger.info('Запускаю скрипт update_deals_from_amo_crm.py')
amo_robot = AmoCRM.AmoCRM()
bq_robot = GoogleBQ_deals.GoogleBQ_deals()
head = bq_robot.get_deals_table_head()
pipelines_list = [7038, 28752] #идентификаторы воронок, из которых хотим забирать сделки
#Получаем из BigQuery дату последнего обновления сделок
updated_at_date = bq_robot.get_last_updated_date()
logger.info(f'Дата последнего обновления: {updated_at_date}')
#обновляем данные и получаем результат merge
df_updated_deals = amo_robot.get_updated_deals_dataframe_from_crm(updated_at_date, head, pipelines_list)
logger.info(f'Обновлены данные Amo Week JSON')
#отправляем результирующий датафрейм в BigQuery
bq_robot.merge_deals_dataframe_to_bigquery(df_updated_deals)
logger.info('Скрипт update_deals_from_amo_crm.py успешно завершён')