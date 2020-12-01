#google_bigquery.py
import os
import config
import pandas as pd
from google.oauth2 import service_account
from pandas.io import gbq
from google.cloud import bigquery
import json
import requests
from loguru import logger
import time


# Указываем идентификатор проекта в Google Cloud
PROJECT_ID = config.GOOGLE_CLOUD_PROJECT_ID
#Прописываем адрес к файлу с данными по сервисному аккаунту и получаем credentials для доступа к данным
creds = service_account.Credentials.from_service_account_file(config.API_KEYS_PATH + config.GOOGLE_CREDENTIALS_JSON_FILE_NAME)
#Загружаем в переменную окружения путь к файлу авторизации гугла. Для работы по API
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.API_KEYS_PATH + config.GOOGLE_CREDENTIALS_JSON_FILE_NAME

#создаём клиента для работы с базой
client = bigquery.Client(project=PROJECT_ID, credentials=creds)

###================================================================
### Загрузка таблиц в BigQuery
###----------------------------------------------

#Создание/перезапись пустой таблицы в BigQuery
#Если таблица уже существует, то ничего не происходит
def create_table(dataset_id, table_id, schema):
    table = bigquery.Table(PROJECT_ID + '.' + dataset_id + '.' + table_id, schema)
    table = client.create_table(table, exists_ok = True)

#Дополнение JSON в BigQuery



#Получаем максимальное значение заданного поля заданной таблицы
#возвращает string
#используем, например, для получения даты последнего обновления сделок
def get_max_field(dataset_id, table_id, field_name):
    QUERY = "SELECT MAX(" + field_name + ") FROM " + dataset_id + "." + table_id + ";"
    result = client.query(QUERY).result().to_dataframe().reset_index()
    return result.iloc[0,1]



#По мотивам статьи - https://habr.com/ru/post/504180/re
#Новые данные сначала выгружаем во временную табличку на BigQuery,
#а потом мержим ей с основной таблицей средствами BigQuery

##Создание таблицы BQ из датафрейма
def create_table_from_dataframe(df, dataset_id, table_id, schema):
    job_config = bigquery.LoadJobConfig()
    #перезаписываем таблицу, если она есть
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
    job_config.schema = schema
    #job_config.autodetect = True #если нужно автоопределение схемы таблицы    
    job = client.load_table_from_dataframe(df, dataset_id + '.' + table_id, job_config=job_config)
    #Ждём результат
    job.result()

#Мерджим временную таблицу в основную
def merge_bq_tables(target_dataset_id, source_dataset_id, target_table_id, source_table_id, schema, head, key):
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

#Обновление BigQuery таблицы из датафрейма - основная функция блока
def merge_bq_table_with_dataframe(df, target_dataset_id, target_table_id, schema, head, key):
    temp_dataset_id = config.BQ_DS_TEMP
    temp_table_id = 'temp_' + target_table_id
    #Грузим датафрейм во временную таблицу
    create_table_from_dataframe(df, temp_dataset_id, temp_table_id, schema)
    
    #Объединяем временную таблицу с основной
    merge_bq_tables(target_dataset_id, temp_dataset_id, target_table_id, temp_table_id, schema, head, key)



###==========================
###==========================
###===========================
def sample(table_id, schema):
    table = bigquery.Table(table_id, schema=schema)
    table = client.create_table(table)  # Make an API request.
    print(
        "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
    )  
    
    # Perform a query.
    QUERY = (
        'SELECT name FROM `bigquery-public-data.usa_names.usa_1910_2013` '
        'WHERE state ="TX" '
        'LIMIT 10')
    query_job = client.query(QUERY)  # API request
    rows = query_job.result()  # Waits for query to finish

    #Пример работающего запроса на склеивание таблиц
    merge_query = """
        MERGE into ds_synapse_analytics.tb_amo_deals_raw as T
        USING ds_temp.tb_amo_deals_raw as S
        ON T.id = S.id
        WHEN MATCHED then UPDATE SET
        T.amo_pipeline_id = S.amo_pipeline_id
        T.updated_at=S.updated_at,
        T.amo_city=S.amo_city
        WHEN NOT MATCHED then INSERT
        (amo_city, created_at, id) VALUES (S.amo_city, S.created_at, S.id)
        """



    for row in rows:
        print(row.name)

def sample_get_table(dataset_id, table_id):
    rows = list(client.list_rows(project_id + '.' + dataset_id + '.' + table_id))
    print("Downloaded {} rows from table {}".format(len(rows), table_id))

def upload_table_from_df(df, dataset_id, table_id, schema):
    try:
        df.to_gbq(destination_table=dataset_id + '.' + table_id, table_schema=schema, project_id=project_id, if_exists='replace', credentials=creds)
        logger.info(f'таблица {table_id} создана в BigQuery')
    except:
        logger.error(f'ошибка создания таблицы {table_id} в BigQuery')