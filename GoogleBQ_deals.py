### GoogleBQ_deals.py - класс для работы со сделками в Google BigQuery 
import datetime as dt
import json
import os
import pandas as pd
import dotenv
import requests
from google.oauth2 import service_account
from google.cloud import bigquery

import config #на время разработки. Там загружается токен API AmoCRM
import pprint

from loguru import logger

class GoogleBQ_deals:
    """
    Взаимодействие с Google BigQuery API
    Загрузка, обработка и выгрузка информации о сделках
    """
    CONFIG = {
        'AMO_DEAL_FIELDS_SCHEMA': 'amo_deal_fields_schema.json',#файл со схемой полей сделок Amo
        'GC_PROJECT_ID': 'synapse-analytics', #Идентификатор проекта в Google Cloud
        'API_KEYS_PATH': 'api_keys/', #путь к ключам аутентификации
        'GOOGLE_CREDENTIALS_JSON_FILE_NAME': 'google_api_creds.json', #ключ аутентификации
        'BQ_DS_MAIN': 'ds_synapse_analytics', #основной датасет в BigQuery
        'BQ_DS_TEMP': 'ds_temp', #датасет для временных таблиц в BigQuery
        'BQ_TB_AMO_DEALS': 'tb_amo_deals' #Имя таблицы для хранения данных по сделке
    }


    def __init__(self, config=None):
        self.CONFIG = {}
        if config:
            self.CONFIG.update(config)
        else:
            self.CONFIG.update(GoogleBQ_deals.CONFIG)
        #Загружаем в переменную окружения путь к файлу авторизации гугла. Для работы по API
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = self.CONFIG['API_KEYS_PATH'] + self.CONFIG['GOOGLE_CREDENTIALS_JSON_FILE_NAME']
        #Прописываем адрес к файлу с данными по сервисному аккаунту и получаем credentials для доступа к данным
        self.creds = service_account.Credentials.from_service_account_file(self.CONFIG['API_KEYS_PATH'] + self.CONFIG['GOOGLE_CREDENTIALS_JSON_FILE_NAME'])
        #создаём клиента для работы с базой
        self.client = bigquery.Client(project=self.CONFIG['GC_PROJECT_ID'], credentials=self.creds)
        self.main_dataset = self.client.dataset(self.CONFIG['BQ_DS_MAIN'])
        self.main_deals_table = self.main_dataset.table(self.CONFIG['BQ_TB_AMO_DEALS'])

    
    #======================================================
    #По мотивам статьи - https://habr.com/ru/post/504180/re
    #Новые данные сначала выгружаем во временную табличку на BigQuery,
    #а потом мержим ей с основной таблицей средствами BigQuery

    #Проверка существования таблицы сделок в bigQuery
    def _if_deals_tbl_exists(self):
        from google.cloud.exceptions import NotFound
        try:
            self.client.get_table(self.main_deals_table)
            return True
        except NotFound:
            return False

    ##Создание таблицы BQ из датафрейма
    def _create_table_from_dataframe(self, df, dataset_id, table_id, schema):
        job_config = bigquery.LoadJobConfig()
        #перезаписываем таблицу, если она есть
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        job_config.schema = schema
      #  job_config.autodetect = True #если нужно автоопределение схемы таблицы    
        job = self.client.load_table_from_dataframe(df, dataset_id + '.' + table_id, job_config=job_config)
        #Ждём результат
        job.result()

    #Мерджим временную таблицу в основную
    def _merge_bq_tables(self, target_dataset_id, target_table_id, source_dataset_id, source_table_id, head, key):
        #head - список столбцов таблицы
        merge_query = "MERGE into " + target_dataset_id + "." + target_table_id + " as T \
            USING " + source_dataset_id + "." + source_table_id + " as S \
            ON T." + key + " = S." + key + \
            " WHEN MATCHED then UPDATE SET " + \
            ', '.join(['T.'+item+'=S.'+item for item in head]) + \
            " WHEN NOT MATCHED then INSERT ("+', '.join(head)+") VALUES (" + ', '.join(['S.'+item for item in head]) + ");"
        job = self.client.query(merge_query)#, job_config=job_config)
        rows = job.result()  # Waits for query to finish

    #Выполняем SQL запрос в BigQuery
    def _make_sql_request(self, query):
        job = self.client.query(query)
        result = job.result() #дожидаемся выполнения запроса
        return result

    #========================
    #Публичные методы
    #---------------------
    #Получение схемы таблицы сделок
    def get_deals_table_schema(self):
    #Возвращает JSON-схему таблицы для BigQuery
        #Читаем json-файл общей схемы сделок
        with open(self.CONFIG['AMO_DEAL_FIELDS_SCHEMA'], 'r', encoding="utf8") as json_file:
            amo_deal_fields = json.load(json_file)
        #Собираем схему для BigQuery
        table_schema = []
        for item in amo_deal_fields['base_amo_deal_fields']['fields']:
            table_schema.append({"name": item['name'],
                                 "type": item['type'],
                                 "mode": item['mode']
                                 })
        for item in amo_deal_fields['custom_amo_deal_fields']['fields']:
            table_schema.append({"name": item['name'],
                                 "type": item['type'],
                                 "mode": item['mode']
                                 })
        for item in amo_deal_fields['_embedded_amo_deal_fields']['fields']:
            table_schema.append({"name": item['name'],
                                 "type": item['type'],
                                 "mode": item['mode']
                                 })
        for item in amo_deal_fields['special_amo_deal_fields']['fields']:
            table_schema.append({"name": item['name'],
                                 "type": item['type'],
                                 "mode": item['mode']
                                 })
        return table_schema

    #Получение даты последнего обновления сделок
    def get_last_updated_date(self):
    #Возвращает самую старшую дату поля updated_at таблицы сделок
     #   QUERY = "SELECT MAX(updated_at) FROM " + self.CONFIG['BQ_TB_AMO_DEALS'] + "." + self.CONFIG['BQ_TB_AMO_DEALS'] + ";"
        QUERY = "SELECT MAX(updated_at) FROM ds_synapse_analytics.tb_amo_deals;"
        result = self._make_sql_request(QUERY).to_dataframe()
        #получаем единственное значение результата запроса
        result = result.reset_index().iloc[0,1]
        return result

    #Получение списка столбцов таблицы сделок
    def get_deals_table_head(self):
        #Читаем json-файл общей схемы сделок
        with open(self.CONFIG['AMO_DEAL_FIELDS_SCHEMA'], 'r', encoding="utf8") as json_file:
            amo_deal_fields = json.load(json_file)
        table_head = []
        for item in amo_deal_fields['base_amo_deal_fields']['fields']:
            table_head.append(item['name'])
        for item in amo_deal_fields['custom_amo_deal_fields']['fields']:
            table_head.append(item['name'])
        for item in amo_deal_fields['_embedded_amo_deal_fields']['fields']:
            table_head.append(item['name'])
        for item in amo_deal_fields['special_amo_deal_fields']['fields']:
            table_head.append(item['name'])
        return table_head

    #Добавляем датафрейм в таблицу сделок
    def merge_deals_dataframe_to_bigquery(self, df):
        schema = self.get_deals_table_schema()
        head = self.get_deals_table_head()
        key = 'amo_deal_id'
        #Если таблица сделок существует
        if self._if_deals_tbl_exists():
            #создаём временную таблицу в bigquery
            self._create_table_from_dataframe(df, self.CONFIG['BQ_DS_TEMP'], self.CONFIG['BQ_TB_AMO_DEALS'], schema)
            logger.info(f"создана временная таблица {self.CONFIG['BQ_DS_TEMP']}.temp_{self.CONFIG['BQ_TB_AMO_DEALS']}")
            #мерджим временную таблицу в существующую
            self._merge_bq_tables(self.CONFIG['BQ_DS_MAIN'], self.CONFIG['BQ_TB_AMO_DEALS'], self.CONFIG['BQ_DS_TEMP'], self.CONFIG['BQ_TB_AMO_DEALS'], head, key)
        #Если таблица сделок отсутствует
        else:
            #создаём и заполняем таблицу сделок
            self._create_table_from_dataframe(df, self.CONFIG['BQ_DS_MAIN'], self.CONFIG['BQ_TB_AMO_DEALS'], schema)
            logger.info(f"создана таблица сделок {self.CONFIG['BQ_DS_MAIN']}.{self.CONFIG['BQ_TB_AMO_DEALS']}")



if __name__ == "__main__":
    robot_bq = GoogleBQ()
    schema = robot_bq.get_deals_table_schema()
    pprint.pprint(schema)