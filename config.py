#config.py
import os
# settings.py
from dotenv import load_dotenv
from pathlib import Path  # Python 3.6+ only

print('config подключен')

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path)

GOOGLE_CLOUD_PROJECT_ID = 'synapse-analytics'
API_KEYS_PATH = 'api_keys/'
GOOGLE_CREDENTIALS_JSON_FILE_NAME = 'google_api_creds.json'

AMO_LEADS_RAW_JSON_PATH = 'amo_leads_raw_json/'
AMO_LEADS_TEMP_JSON_PATH = 'amo_leads_temp_json/'
AMO_LEADS_WEEK_JSON_PATH = 'amo_leads_week_json/'
AMO_ALL_LEADS_EXT_JSON_FILENAME = 'amocrm_all_leads_ext_json'
AMO_PAGE_SIZE = 250 #Количество сделок, запрашиваемых из Amo за один запрос
AMO_PAGES_COUNT_PER_LOAD = 50 #Количество страниц размером AMO_PAGE_SIZE, которое будем подгружать за один запуск скрипта (для случаем, когда нам нужно выгрузить из AMO много сделок)
AMO_PAUSE_BETWIN_REQUESTS = 2 #Пауза, между запросами пачек сделок по API AmoCRM

AMO_SUBDOMAIN = 'syn'
#Базовые поля сделок в Амо
AMO_DEALS_BASE_FIELDS = {'id', 
                         'created_at', 
                         'updated_at', 
                         'amo_pipeline_id', 
                         'amo_status_id',
                         'amo_price',
                         'amo_responsible_user_id',
                         'amo_group_id',
                         'amo_loss_reason_id',
                         'amo_created_by',
                         'amo_updated_by',
                         'amo_closed_at',
                         'amo_is_deleted'}
           
#Пользовательские поля в Амо с кодами полей
AMO_DEALS_CUSTOM_FIELDS = {'amo_city': 512318, #город клиента, проставляемый менеджерами в AmoCRM
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
                  'amo_channel': 600935,
                  'amo_items_2019': 562024, #Старое значение поля Услуга в AmoCRM
                  'amo_items_2020': 648028, #Новое значение поля Услуга в AmoCRM
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
                  'ct_call_id': 648282}
#Поля, добавляемые скриптом, при выгрузке сделок из Amo и значения по-умолчанию, если такого поля в JSON нет
AMO_DEALS_SPECIAL_FIELDS ={'trashed_at':None}
AMO_PIPELINES_ID = {"CNTX": 7038,
                    "WEB": 28752}
#pipeline_id: trash_status_id
AMO_TRASH_STATUSES_ID = {7038: 28985871, #CNTX
                   28752: 29160522}  #WEB

BQ_DEALS_SPECIAL_FIELDS = {'created_at_week',
                           'created_at_month',
                           'created_at_year',
                           'created_at_bq_timestamp',
                           'closed_at_bq_timestamp',
                           'trashed_at_bq_timestamp',
                           'lead_utm_source',
                           'lead_utm_medium',
                           'lead_utm_campaign',
                           'lead_utm_content',
                           'lead_utm_term'}

BQ_DS_MAIN = 'ds_synapse_analytics'
BQ_DS_TEMP = 'ds_temp'
BQ_TB_AMO_DEALS_RAW = 'tb_amo_deals_raw'

BQ_TB_AMO_DEALS_RAW_SHEMA = 'bq_tb_amo_deals_raw_shema.json'    


#==============
#Яндекс Директ
YADIR_TOKEN = 'AgAAAAACXLH1AAafG9bDBXhUlE-SoDOyU-epWVA'