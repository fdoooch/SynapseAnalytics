#put_all_deals_from_amo_crm_to_gbq.py
#Выгружаем все сделки из AmoCRM и загружаем их в Google BigQuery
#from google.cloud import bigquery as gbq
#
#import google_bigquery as gb
import AmoCRM
import os
import config
import pandas as pd
import json
from google.cloud import bigquery
from google.oauth2 import service_account
import google_bigquery as gb
from loguru import logger


#По мотивам статьи - https://habr.com/ru/post/504180/re
#Новые данные сначала выгружаем во временную табличку на BigQuery,
#а потом мержим ей с основной таблицей средствами BigQuery

##Создание таблицы BQ из датафрейма
def create_table_from_dataframe(df, dataset_id, table_id):
    job_config = bigquery.LoadJobConfig()
    #перезаписываем таблицу, если она есть
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    #job_config.schema = schema
    job_config.autodetect = True #если нужно автоопределение схемы таблицы    
    job = client.load_table_from_dataframe(df, dataset_id + '.' + table_id, job_config=job_config)
    #Ждём результат
    job.result()

#Мерджим временную таблицу в основную
def merge_bq_tables(target_dataset_id, source_dataset_id, target_table_id, source_table_id, head, key):
    #head - список столбцов таблицы
    merge_query = "MERGE into " + target_dataset_id + "." + target_table_id + " as T \
        USING " + source_dataset_id + "." + source_table_id + " as S \
        ON T." + key + " = S." + key + \
        " WHEN MATCHED then UPDATE SET " + \
        ', '.join(['T.'+item+'=S.'+item for item in head]) + \
        " WHEN NOT MATCHED then INSERT ("+', '.join(head)+") VALUES (" + ', '.join(['S.'+item for item in head]) + ");"

    job = client.query(merge_query)#, job_config=job_config)
    rows = job.result()  # Waits for query to finish
    for row in rows:
        print(row.id, row.created_at, row.amo_city)

#Обновление BigQuery таблицы из датафрейма
def merge_dataframe_to_google_big_query(df, target_dataset_id, target_table_id, head, key):
    temp_dataset_id = config.BQ_DS_TEMP
    temp_table_id = 'temp_' + target_table_id
    #Грузим датафрейм во временную таблицу
    create_table_from_dataframe(df, temp_dataset_id, temp_table_id)
    #Объединяем временную таблицу с основной
    merge_bq_tables(target_dataset_id, temp_dataset_id, target_table_id, temp_table_id, head, key)


# Указываем идентификатор проекта в Google Cloud
PROJECT_ID = config.GOOGLE_CLOUD_PROJECT_ID
#Прописываем адрес к файлу с данными по сервисному аккаунту и получаем credentials для доступа к данным
creds = service_account.Credentials.from_service_account_file(config.API_KEYS_PATH + config.GOOGLE_CREDENTIALS_JSON_FILE_NAME)
#Загружаем в переменную окружения путь к файлу авторизации гугла. Для работы по API
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.API_KEYS_PATH + config.GOOGLE_CREDENTIALS_JSON_FILE_NAME

#создаём клиента для работы с базой
client = bigquery.Client(project=PROJECT_ID, credentials=creds)


logger.add("info.log", format="{time} {level} {module} : {function} - {message}", level="INFO", rotation="10:00", compression="zip")
amo_robot = AmoCRM.AmoCRM()

logger.info('Запускаю скрипт put_all_deals_from_amo_crm_to_gbq.py')
logger.info('Выгружаю все сделки из AmoCRM в raw_json-файлы')
#amo_robot.get_all_deals_from_crm()
logger.info('Все сделки выгружены')
for year in range(2015, 2016):
    logger.info(f'Собираю датафрейм за {year} год')
    #df_deals = amo_robot.get_deals_dataframe_by_year(year)
    df_deals = amo_robot.get_deals_dataframe_by_week(2020, 40)
    target_dataset_id = config.BQ_DS_MAIN
    target_table_id = 'tb_test_table'
    head = amo_robot.get_amo_deals_rows_schema
    key = 'id'

    merge_dataframe_to_google_big_query(df_deals, target_dataset_id, target_table_id, head, key)
#df = amo_robot.get_deals_dataframe_by_year(year)
#print(df)