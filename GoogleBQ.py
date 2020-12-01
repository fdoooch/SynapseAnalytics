### GoogleBQ.py - класс для работы с Google BigQuery
import datetime as dt
import json
import os
import pandas as pd
import dotenv
import config #на время разработки. Там загружается токен API AmoCRM
import requests
import pprint

from loguru import logger

class GoogleBQ:
    """
    Взаимодействие с Google BigQuery API
    Загрузка, обработка и выгрузка информации о сделках
    """
    CONFIG = {
        'AMO_DEAL_FIELDS_SCHEMA': 'amo_deal_fields_schema.json',#файл со схемой полей сделок Amo
        'LEADS_TEMP_JSON_PATH': 'amo_leads_temp_json/',
        'LEADS_WEEK_JSON_PATH': 'amo_leads_week_json/',
        'JSON_DEALS_RAW_FILENAME': 'json_deals_raw',
        'JSON_DEALS_WEEK_FILENAME': 'json_deals_week',
        'ALL_LEADS_EXT_JSON_FILENAME': 'amocrm_all_leads_ext_json',
        'PAGE_SIZE': 250,#Количество сделок, запрашиваемых из Amo за один запрос
        'PAGES_COUNT_PER_LOAD': 50, #Количество страниц размером AMO_PAGE_SIZE, которое будем подгружать за один запуск скрипта (для случаем, когда нам нужно выгрузить из AMO много сделок)
        'PAUSE_BETWIN_REQUESTS': 2, #Пауза, между запросами пачек сделок по API AmoCRM
        'SUBDOMAIN': 'syn',
        'AMO_ACCESS_TOKEN': '',
        'AMO_REDIRECT_URI': 'https://hook.integromat.com/78sigwp948jnsjf2ndodfctwc3yuechm',
        'AMO_API_REQUESTS_HEADERS': {'User-Agent': 'amoCRM-oAuth-client/1.0',
                  'Content-Type': 'application/json',
                  'Authorization': 'Bearer ' + 'AMO_ACCESS_TOKEN'},

        'AMO_API_REQUESTS_ERRORS': {
                        400: 'Bad request',
                        401: 'Unauthorized',
                        403: 'Forbidden',
                        404: 'Not found',
                        500: 'Internal server error',
                        502: 'Bad gateway',
                        503: 'Service unavailable'},
        
        'TIME_FORMAT': '%Y-%m-%d %H:%M:%S',
        'WEEK_OFFSET': dt.timedelta(hours=24 - 24 + 6 - 6), #Сдвиг начала недели (для отчётов с нестандартными неделями)
        'AMO_DEALS_BASE_FIELDS': {'id', 
                         'created_at', 
                         'updated_at', 
                         'pipeline_id', 
                         'status_id',
                         'price',
                         'responsible_user_id',
                         'group_id',
                         'loss_reason_id',
                         'created_by',
                         'updated_by',
                         'closed_at',
                         'is_deleted'},

        #Пользовательские поля в Амо с кодами полей
        'AMO_DEALS_CUSTOM_FIELDS': {'city': 512318, #город клиента, проставляемый менеджерами в AmoCRM
                  'tilda_city':648980, #город, на который был ориентирован лендинг на Тильде
                  'ct_city': 648274, #город, определившийся CallTouch
                  'drupal_utm': 632884, #Строка с utm-метками, пробрасываемая Друпалом в Амо
                  'tilda_utm_source': 648158, #utm_source, пробрасываемая Тильдой в Амо
                  'tilda_utm_medium': 648160,
                  'tilda_utm_campaign': 648310,
                  'tilda_utm_content': 648312,
                  'tilda_utm_term': 648314,
                  'ct_utm_source': 648256, #utm_source, проставляемый CallTouch
                  'ct_utm_medium': 648258,
                  'ct_utm_campaign': 648260,
                  'ct_utm_content': 648262,
                  'ct_utm_term': 648264,
                  'channel': 600935,
                  'items_2019': 562024, #Старое значение поля Услуга в AmoCRM
                  'items_2020': 648028, #Новое значение поля Услуга в AmoCRM
                  'tilda_product': 648152, #Метка продукта в Тильде
                  'drupal_piwik_id': 589816,#piwik_id пробрасываемый Друпалом
                  'tilda_piwik_id': 648530,
                  'drupal_google_id': 589818,
                  'ct_google_id': 648292,
                  'ct_yandex_id':648294,
                  'ct_calltouch_session_id': 648288,
                  'tilda_calltouch_session_id': 648532,
                  'ct_calltouch_client_id': 648290,
                  'tilda_cookies': 648166,
                  'drupal_page': 587868, 
                  'tilda_page':648556,
                  'ct_page': 648268,
                  'tilda_form_id': 648164,
                  'tilda_form_name': 648162,
                  'tilda_referer': 648168,
                  'ct_referer': 648266,
                  'ct_create_from': 648218,
                  'ct_type_communication': 648220,
                  'ct_client_phone_number': 648224,
                  'ct_communication_date': 648232,
                  'ct_communication_time': 648234,
                  'ct_call_long': 648236,
                  'ct_call_waiting': 648238,
                  'ct_device': 648276,
                  'ct_os': 648278,
                  'ct_browser': 648280,
                  'ct_call_id': 648282},

        #Идентификаторы воронк AmoCRM
        'AMO_PIPELINES_ID': {"CNTX": 7038,
                    "WEB": 28752},

        #pipeline_id: trash_status_id
        'AMO_TRASH_STATUSES_ID': {7038: 28985871, #CNTX
                   28752: 29160522},  #WEB

        #Поля, вычисляемые из полей сделок в AmoCRM
        'DEALS_SPECIAL_FIELDS': {'created_at_timestamp',
                           'updated_at_timestamp'
                           'trashed_at',
                           'lead_utm_source',
                           'lead_utm_medium',
                           'lead_utm_campaign',
                           'lead_utm_content',
                           'lead_utm_term',
                           'lead_device',
                           'lead_browser',
                           'lead_os'}
    }    

    def __init__(self, config=None):
        self.CONFIG = {}
        if config:
            self.CONFIG.update(config)
        else:
            self.CONFIG.update(GoogleBQ.CONFIG)
        self.CONFIG['AMO_ACCESS_TOKEN'] = os.getenv('AMO_ACCESS_TOKEN')
        self.CONFIG['AMO_API_REQUESTS_HEADERS']['Authorization'] = 'Bearer ' + os.getenv('AMO_ACCESS_TOKEN')


    #========================
    #Получение схемы таблицы сделок
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



if __name__ == "__main__":
    robot_bq = GoogleBQ()
    schema = robot_bq.get_deals_table_schema()
    pprint.pprint(schema)