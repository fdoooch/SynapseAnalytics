### AmoCRM.py - класс для работы с AmoCRM
import datetime as dt
import json
import os
import pandas as pd
import dotenv
import config #на время разработки. Там загружается токен API AmoCRM
import requests
import operator
from time import sleep

from loguru import logger

class AmoCRM:
    """
    Взаимодействие с AmoCRM API
    Загрузка, обработка и выгрузка информации о сделках
    """
    CONFIG = {
        'LEADS_RAW_JSON_PATH': 'amo_leads_raw_json/',
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
            self.CONFIG.update(AmoCRM.CONFIG)
        self.CONFIG['AMO_ACCESS_TOKEN'] = os.getenv('AMO_ACCESS_TOKEN')
        self.CONFIG['AMO_API_REQUESTS_HEADERS']['Authorization'] = 'Bearer ' + os.getenv('AMO_ACCESS_TOKEN')

    #========================
    #Авторизация в AMO CRM
    #---------------------
    #обновление токена
    def _refresh_access_token(self):
        url = 'https://hook.integromat.com/957pemos5degb894sr6kocv4dxtmbuql'
        params = {'pass': os.getenv('FDPASS')}
        headers = {
        "User-Agent": "Synapse_user_agent",
        "Content-Type": "application/json"} 
        rs = requests.post(url, params=params, headers=headers)
        print(rs.text)
        os.environ['AMO_ACCESS_TOKEN'] = json.loads(rs.text)['access_token']
        dotenv_file = dotenv.find_dotenv()
        dotenv.set_key(dotenv_file, 'AMO_ACCESS_TOKEN', json.loads(rs.text)['access_token'])
        dotenv.set_key(dotenv_file, 'AMO_REFRESH_TOKEN', json.loads(rs.text)['refresh_token'])

    #--------------------
    #Конец блока авторизации в Amo CRM
    #=======================


    #Получаю пустую таблицу сделок AMO
    def get_amo_deals_rows_schema(self):
    #требуется для того, чтобы все наши таблицы имели одинаковый набор столбцов
    #после заполнения таблицы требуется удалить из неё нулевую строку (она заполнена пустыми значениями)
        row = {}
        for field in self.CONFIG['AMO_DEALS_BASE_FIELDS']:
            row.update({field: []})
        for field in self.CONFIG['AMO_DEALS_CUSTOM_FIELDS']:
            row.update({field: []})
        for field in self.CONFIG['DEALS_SPECIAL_FIELDS']:
            row.update({field: []})
        schema = []
        schema.append(row)
        return schema
    
    #Парсинг json_deals в набор строк
    #каждая строка - информация об одной сделке
    def _extract_amo_json_deals_to_rows(self, json_deals):
        rows = []
        #Проходим по сделкам и наполняем amo_deals rows
        for deal in json_deals:
            rows.append(self._get_row_from_amo_json_deal(deal))
        return rows
    
    #Наполняю таблицу сделками из JSON-файла AMO JSON DEALS
    def extract_amo_json_file_to_rows(self, json_path, json_filename):
        #Читаем json-файл
        logger.debug(f'Читаем файл {json_path + json_filename}')
        with open(json_path + json_filename, 'r', encoding="utf8") as json_file:
            json_deals = json.load(json_file)
        rows = self._extract_amo_json_deals_to_rows(json_deals)
        return rows

    #Создаю таблицу сделок из всех JSON-файлов в директории
    def extract_amo_json_from_directory_to_rows(self, json_path):
        files_list = os.listdir(json_path)
        logger.debug(f'Список файлов для загрузки: {files_list}')
        rows = []
        for filename in files_list:
            rows = rows + self.extract_amo_json_file_to_rows(json_path, filename)
        return rows

    #Получаю словарь field_id: values из json_deal
    def _get_custom_values_dict_from_custom_field_values_json(self, json_cfv):
        cfv_dict = {}
        for field in json_cfv:
            if field['field_id'] == 648028:
                #Если это поле со списком товаров (648028), то сериализуем его
                cfv_dict[field['field_id']] = json.dumps(field['values'], ensure_ascii=False)
                #cfv_dict[field['field_id']] = json.dumps(field['values'][0]['value'], ensure_ascii=False)
            else:
                cfv_dict[field['field_id']] = field['values'][0]['value']
        return cfv_dict

    #Получаю словарь utm-меток из словаря custom
    #здесь хардкод id-полей, содержащих utm-метки
    #если поля меняются, нужно будет переписать эту функцию
    def _get_lead_utm_from_custom_values_dict(self, cf_dict):
        utm_dict = {}
        if 632884 in cf_dict:
            try:
                drupal_utm_dict = self._get_drupal_utm_dict(cf_dict[632884])
                utm_dict['lead_utm_source'] = drupal_utm_dict['source']
                utm_dict['lead_utm_medium'] = drupal_utm_dict['medium']
                utm_dict['lead_utm_campaign'] = drupal_utm_dict['campaign']
                utm_dict['lead_utm_content'] = drupal_utm_dict['content']
                utm_dict['lead_utm_term'] = drupal_utm_dict['keyword']
            except:
                logger.error(f'Ошибка в utm_метке: {cf_dict[632884]}')
        if not 'lead_utm_source' in utm_dict:
            if 648256 in cf_dict:
                utm_dict['lead_utm_source'] = cf_dict[648256] #ct_utm_source
            else:
                utm_dict['lead_utm_source'] = cf_dict.get(648158, '') #tilda_utm_source

        if not 'lead_utm_medium' in utm_dict:
            if 648258 in cf_dict:
                utm_dict['lead_utm_medium'] = cf_dict[648258] #ct_utm_medium
            else:
                utm_dict['lead_utm_medium'] = cf_dict.get(648160, '') #tilda_utm_medium

        if not 'lead_utm_campaign' in utm_dict:
            if 648260 in cf_dict:
                utm_dict['lead_utm_campaign'] = cf_dict[648260] #ct_utm_campaign
            else:
                utm_dict['lead_utm_campaign'] = cf_dict.get(648310, '') #tilda_utm_campaign

        if not 'lead_utm_content' in utm_dict:
            if 648262 in cf_dict:
                utm_dict['lead_utm_content'] = cf_dict[648262] #ct_utm_content
            else:
                utm_dict['lead_utm_content'] = cf_dict.get(648312, '') #tilda_utm_content

        if not 'lead_utm_term' in utm_dict:
            if 648264 in cf_dict:
                utm_dict['lead_utm_term'] = cf_dict[648264] #ct_utm_term
            else:
                utm_dict['lead_utm_term'] = cf_dict.get(648314, '') #tilda_utm_term

        return utm_dict

    #Парсим поле drupal_utm в словарь drupal_utm_dict
    def _get_drupal_utm_dict(self, drupal_utm):
        drupal_utm_list = drupal_utm.split(', ')
        drupal_utm_dict = dict(
                [item.split('=') for item in drupal_utm_list if '=' in item]
        )
        #Проверяем не поменяны ли местами метки (в старой статистике такое было)
        try:
            if drupal_utm_dict['medium'] in ['yandex', 'google']:
                #меняем местами source <=> medium
                s = drupal_utm_dict['source']
                drupal_utm_dict['source'] = drupal_utm_dict['medium']
                drupal_utm_dict['medium'] = s
        except:
            logger.error(f'Ошибка в поле utm_метка: {drupal_utm}')
        return drupal_utm_dict

    #Собираю строку таблицы из данных сделки amo_json
    #hardcode!!!
    def _get_row_from_amo_json_deal(self, json_deal):
        row = {}
        #Добавляем базовые поля
        for field in self.CONFIG['AMO_DEALS_BASE_FIELDS']:
            row.update({field: json_deal[field]})
        #Если есть пользовательские поля, добавляем их
        custom_fields_values = json_deal.get('custom_fields_values')
        if custom_fields_values:
            #составляем словарь field_id: value
            custom_fields_dict = self._get_custom_values_dict_from_custom_field_values_json(custom_fields_values)
            for key, value in self.CONFIG['AMO_DEALS_CUSTOM_FIELDS'].items():
                if value in custom_fields_dict:
                    row[key] = custom_fields_dict[value]#[0].get('value')
            #Вытаскиваем значения UTM-меток в единые поля lead_utm_...
            lead_utms = self._get_lead_utm_from_custom_values_dict(custom_fields_dict)
            row.update(lead_utms)

        #Добавляем дополнительные поля
        row['created_at_timestamp'] = pd.to_datetime(row['created_at'], unit='s')
        row['updated_at_timestamp'] = pd.to_datetime(row['updated_at'], unit='s')
        row['trashed_at'] = json_deal.get('trashed_at', None)
        return row

    #Получаю датафрейм сделок из JSON
    def _extract_amo_json_deals_to_dataframe(self, json_deals):
        rows = self._extract_amo_json_deals_to_rows(json_deals)
        df = pd.DataFrame(rows)
        return df

    #=====================
    # Работа над получением и обновлением сделок из AmoCRM
    #=====================

    #API Скачиваем все сделки из AmoCRM и раскладываем их в набор файлов JSON
    def _get_all_deals_ext_to_json(self, raw_json_path, json_raw_deals_filename):
    #Выкачиваем все сделки со списками из AMO отсортированные по дате создания по возрастанию и сохраняем их в JSON-файл
    #Разбиваем запрос на пачки, чтобы Amo нас не блокировало
    #Эту функцию используем тогда, когда у нас на сервере нет никакого списка сделок
    # json_raw_deals_filename - начало имени, под которым будут сохраняться файлы при скачивании из AmoCRM
    # json_week_deals_filename - начало имени, под которым будут храниться недельные пачки сделок
        page = 1
        #Если папки с raw_json нет - создаём
        if not os.path.isdir(raw_json_path):
            os.mkdir(raw_json_path)
        deals_pack = []
        has_more = True
        while has_more:
            #запрашиваем очередную страницу сделок из Амо
            logger.debug(f'Запрашиваю страницу {page} из AmoCRM')
            rs = self._get_deals_ext_created_date_inc(self.CONFIG['PAGE_SIZE'], page)
            #Если запрос сработал без ошибок
            if rs.status_code == 200:
                #добавляем полученные сделки в JSON коллекцию
                deals_pack += (json.loads(rs.text))['_embedded']['leads']
                #Если количество полученных записей достигла предела - сохраняем их в файл и обнуляем массив
                if page % self.CONFIG['PAGES_COUNT_PER_LOAD'] == 0:
                    with open(raw_json_path + json_raw_deals_filename + str(page) + '.json', 'w', encoding="utf8") as output_file:
                        json.dump(deals_pack, output_file, ensure_ascii=False)
                    deals_pack = []
                    logger.info(f'{str(page)} выгружено в файл')
            elif rs.status_code == 204:
                logger.info('загрузка успешно завершена')
                has_more = False
                break
            else:
                logger.error(f'Ошибка {rs.status_code}')
                has_more = False
                break
            page += 1
            sleep(self.CONFIG['PAUSE_BETWIN_REQUESTS'])
        with open(raw_json_path + json_raw_deals_filename + str(page) + '.json', 'w', encoding="utf8") as output_file:
            json.dump(deals_pack, output_file, ensure_ascii=False)
        return 0

    
    #API Получаем список сделок, отсортированный по возрастанию даты создания   
    def _get_deals_ext_created_date_inc(self, limit, page):
    #API AmoCRM v4
    #Page - номер страницы (размер страницы = limit)
    #Код ответа 204 означает, что контента больше нет
    #Функция возвращает ответ на запрос к API AmoCRM
        url = 'https://' + self.CONFIG['SUBDOMAIN'] + '.amocrm.ru/api/v4/leads'
        params = {
            "limit": limit,
            "page": page,
            "with": "catalog_elements,contacts,loss_reason",
            "order[created_at]": "inc"
        }

        try:
            try_num = 1
            rs = requests.get(url, headers=self.CONFIG['AMO_API_REQUESTS_HEADERS'], params=params)
            while rs.status_code == 401:
                logger.error(f'Не удалось авторизоваться в API AmoCRM. Попытка {try_num}')
                self._refresh_access_token()
                sleep(2 ** try_num)
                rs = requests.get(url, headers=self.CONFIG['AMO_API_REQUESTS_HEADERS'], params=params)
                try_num += 1
            return rs
        except ConnectionError:
            logger.error('Ошибка ConnectionError ' + url)
   
    #API Получаем список сделок, отсортированный по убыванию даты последней модификации
    def _get_deals_ext_sorted_by_updated_date_desc(self, limit, page):
    #API AmoCRM v4
    #Получаем список сделок с элементами списков. limit = 0 .. 500, но лучше 50
    #Отсортирован по убыванию даты последней модификации
    #Page - номер страницы (размер страницы = limit)
    #Код ответа 204 означает, что контента больше нет
    #Функция возвращает ответ на запрос к API AmoCRM
        url = 'https://' + self.CONFIG['SUBDOMAIN'] + '.amocrm.ru/api/v4/leads'
        params = {
            "limit": limit,
            "page": page,
            "with": "catalog_elements,contacts,loss_reason",
            "order[updated_at]": "desc"
        }

        try:
            rs = requests.get(url, headers=self.CONFIG['AMO_API_REQUESTS_HEADERS'], params=params)
            return rs
        except ConnectionError:
            logger.error('Ошибка ConnectionError ' + url)

    #Скачиваем все сделки, обновлённые после даты, во временную папку
    def _get_updated_deals_since_timestamp_to_json_temp_folder(self, last_update):
        page = 1
        limit = self.CONFIG['PAGE_SIZE']
        if not os.path.isdir(config.AMO_LEADS_TEMP_JSON_PATH):
            os.mkdir(config.AMO_LEADS_TEMP_JSON_PATH)
        #Здесь нужно вставить функцию очистки временной директории от файлов
        #
        #
        new_deals = []
        has_more = True
        while has_more:
            rs = self.amo_get_deals_sorted_by_updated_date_desc(limit, page)
            if rs.status_code == 200:
                new_deals += (json.loads(rs.text))['_embedded']['leads']
                #если последняя из загруженных сделок обновлена раньше заданной даты - завершаем скачивание
                if new_deals[-1]['updated_at'] < last_update:
                    has_more = False

                #Если количество полученных записей достигла предела - сохраняем их в файл и обнуляем массив
                if page % self.CONFIG['PAGES_COUNT_PER_LOAD'] == 0:
                    with open(self.CONFIG['LEADS_TEMP_JSON_PATH'] + 'temp_updated_leads_' + str(page) + '.json', 'w', encoding="utf8") as output_file:
                        json.dump(new_leads, output_file, ensure_ascii=False)
                    new_deals = []
                    logger.info(f'{str(page)} выгружено в файл')
            elif rs.status_code == 204:
                logger.info('загрузка преждевременно завершена')
                has_more = False
                break
            else:
                logger.error(f'Ошибка {rs.status_code}')
                has_more = False
                break
            page += 1
            sleep(self.CONFIG['PAUSE_BETWIN_REQUESTS'])
            logger.debug(f'Page: {page}')
        with open(self.CONFIG['LEADS_TEMP_JSON_PATH'] + 'temp_updated_leads_' + str(page), 'w', encoding="utf8") as output_file:
            json.dump(all_leads, output_file, ensure_ascii=False)

        return 0
    
    #Добавляем пакет сделок в нашу базу AMO JSON WEEK (Список сделок в JSON, разбитый на файлы по неделям создания сделки)
    #Возвращаем json результата слияния
    def _merge_json_pack_to_json_week_deals(self, json_pack, week_json_path, week_json_filename):
        #Если базы JSON WEEK нет - создаём
        if not os.path.exists(week_json_path):
            logger.info('База AMO JSON WEEK не найдена')
            os.mkdir(week_json_path)
            logger.info('Создана новая база AMO JSON WEEK в каталоге ' + week_json_path)

        count_added_deals = 0
        count_updated_deals = 0
        result_json = [] #сюда собираем результаты синхронизации сделок

        #Пока в пачке есть сделки добавляем их в базу
        while len(json_pack) > 0:
            new_deal = json_pack[0]
            year = dt.datetime.fromtimestamp(new_deal['created_at']).isocalendar()[0]
            week = dt.datetime.fromtimestamp(new_deal['created_at']).isocalendar()[1]
        
            #Если json с этой недели уже есть, дополняем его
            if os.path.isfile(week_json_path + week_json_filename + '_' + str(year) +'_'+ str(week).zfill(2) + '.json'):
                with open(week_json_path + week_json_filename + '_' + str(year) +'_'+ str(week).zfill(2) + '.json', 'r', encoding="utf8") as week_json_file:
                    logger.info("Дополняем файл " + week_json_path + week_json_filename + '_' + str(year) +'_'+ str(week).zfill(2) + '.json',)
                    week_deals = json.load(week_json_file)
                
                    #Пока у новых сделок сохраняется номер недели
                    while dt.datetime.fromtimestamp(json_pack[0]['created_at']).isocalendar()[0] == year and dt.datetime.fromtimestamp(json_pack[0]['created_at']).isocalendar()[1] == week:
                        #Если сделка с таким id уже содержится в базе JSON WEEK - обновляем её
                        if any(deal['id'] == json_pack[0]['id'] for deal in week_deals):
                            #и обновляем инфу по этой сделке в week_deals
                            week_deals = self._update_deal_in_json_deals(json_pack[0], week_deals)
    #                        logger.info(f'Сделка #{new_deal["id"]} обновлена')
                            count_updated_deals += 1
                        #если id новой сделки уникален - добавляем сделку
                        else:
                            #добавляем инфу по сделке и синхронизируем week_deals с результатом добавления
                            week_deals = self._add_deal_to_json_deals(json_pack[0], week_deals)
     #                       logger.info(f'Сделка #{json_pack[0]["id"]} добавлена в AMO JSON WEEK')
                            count_added_deals += 1
                        #Удаляем добавленную сделку из пачки
                        json_pack.pop(0)
                        #если сделок в пачке больше нет, сохраняем сделки в AMO JSON WEEK и завершаем функцию
                        if len(json_pack) == 0:
                            output_filename = week_json_filename + '_' + str(year) +'_'+ str(week).zfill(2) + '.json'
                            with open(week_json_path + output_filename, 'w', encoding="utf8") as output_file:
                                json.dump(week_deals, output_file, ensure_ascii=False, indent=2)
                            logger.info('Обновлён файл:' + output_filename)
                            logger.info('Добавление пачки сделок завершено')
                            logger.info(f'{count_added_deals} было добавлено')
                            logger.info(f'{count_updated_deals} было обновлено')
                            result_json.append(week_deals)
                            return result_json

                    #Если неделя в новой сделке отличаеся от той, с которой работали, то сохраняем сделки в AMO JSON WEEK и начинаем работу с новой неделей
                    output_filename = week_json_filename + '_' + str(year) +'_'+ str(week).zfill(2) + '.json'
                    with open(week_json_path + output_filename, 'w', encoding="utf8") as output_file:
                        json.dump(week_deals, output_file, ensure_ascii=False, indent=2)
                    logger.info('Обновлён файл:' + output_filename)
                    result_json.append(week_deals)
                    continue

            #Если в AMO JSON WEEK нет файла соответствующей недели, создаём его и наполняем
            else:
                logger.info(f'Добавляем в AMO JSON WEEK новую неделю: {week}, год: {year}')
                week_deals = []
            
                #Пока у новых сделок сохраняется номер недели
                while dt.datetime.fromtimestamp(json_pack[0]['created_at']).isocalendar()[0] == year and dt.datetime.fromtimestamp(json_pack[0]['created_at']).isocalendar()[1] == week:
                    week_deals = self._add_deal_to_json_deals(json_pack[0], week_deals)
                    count_added_deals += 1
                    json_pack.pop(0)
                    #если сделок в пачке больше нет, сохраняем сделки в AMO JSON WEEK и завершаем функцию
                    if len(json_pack) == 0:
                        output_filename = week_json_filename + '_' + str(year) +'_'+ str(week).zfill(2) + '.json'
                        with open(week_json_path + output_filename, 'w', encoding="utf8") as output_file:
                            json.dump(week_deals, output_file, ensure_ascii=False, indent=2)
                        logger.info('Создан файл:' + output_filename)
                        logger.info('Добавление пачки сделок завершено')
                        logger.info(f'{count_added_deals} было добавлено')
                        logger.info(f'{count_updated_deals} было обновлено')
                        result_json.append(week_deals)
                        return result_json
                        

                #Если неделя в новой сделке отличаеся от той, с которой работали, то сохраняем сделки в AMO JSON WEEK и начинаем работу с новой неделей
                output_filename = week_json_filename + '_' + str(year) +'_'+ str(week).zfill(2) + '.json'
                with open(week_json_path + output_filename, 'w', encoding="utf8") as output_file:
                    json.dump(week_deals, output_file, ensure_ascii=False, indent=2)
                logger.info('Обновлён файл:' + output_filename)
                result_json.append(week_deals)
                continue              
    
        #Если в пачке кончились сделки - завершаем функцию
        return result_json

    def _update_deal_in_json_deals(self, new_deal, deals):
        #Если дата изменения соответствующей сделки в AMO JSON WEEK не меньше даты изменения new_deal - ничего не меняем
        #Переставляем сделку в конец JSON
        deal_index = [old_deal['id'] for old_deal in deals].index(new_deal['id'])
        if deals[deal_index]['updated_at'] >= new_deal['updated_at']:
            new_deal = deals[deal_index]
            deals.pop(deal_index)
            deals = self._add_deal_to_json_deals(new_deal, deals)    
    
        #Если дата изменения новой сделки больше, а старая сделка содержит информацию о дате перехода в статус "Не целевой" (trash)
        # - заменяем старую сделку на новую, сохраная дату перехода в статус "Не целевой"
        elif 'trashed_at' in deals[deal_index]:
            new_deal['trashed_at'] = deals[deal_index]['trashed_at']
            deals.pop(deal_index)
            deals = self._add_deal_to_json_deals(new_deal, deals)
    
        #Иначе - просто заменяем старую сделку на новую
        else:
            deals.pop(deal_index)
            deals = self._add_deal_to_json_deals(new_deal, deals)

        return deals

    #добавляем сделку в базу AMO JSON WEEK
    def _add_deal_to_json_deals(self, new_deal, deals):
        #Если сделка в статусе "Треш - нецелевые", то добавляем дату последней модификации в поле ['trashed_at']
        if new_deal['status_id'] in self.CONFIG['AMO_TRASH_STATUSES_ID'] and not 'trashed_at' in new_deal:
            new_deal['trashed_at'] = new_deal['updated_at']
        #deals.append(new_deal)
        deals.append(new_deal)
        return deals

    #Разбираем данные по сделкам, скаченные из Амо в json и раскладываем их в файлы по неделям
    #Возвращаем json результата слияния
    def _put_deals_from_raw_json_to_week_json(self, raw_json_path, week_json_path, week_json_filename):
    ###Упорядочивание архива json-файлов
    ###Перепаковываем сделки в файлы, группирующие их по неделе создания
    ###Название файлов: filename_YYYY_WW.json
    ###Недели нумеруются по ISO
    ###первой неделей года считается неделя, содержащая первый четверг года, что эквивалентно следующим выражениям:
    ### неделя, содержащая 4 января;
    ### неделя, в которой 1 января это понедельник, вторник, среда или четверг;
        files_list = os.listdir(raw_json_path)
        for filename in files_list:    
            with open(raw_json_path + filename, 'r', encoding="utf8") as json_file:
                deals = json.load(json_file)
                deals.sort(key=operator.itemgetter('created_at'))
            #Добавляем новые сделки в AMO JSON WEEK
            deals = self._merge_json_pack_to_json_week_deals(deals, week_json_path, week_json_filename)
            logger.info('загрузка в AMO JSON WEEK завершена')
        return deals

    #Разбираем данные по сделкам, скаченные из Амо во временную папку json
    def _put_deals_from_temp_json_to_week_json(self, week_json_path):
    # и раскладываем их в файлы по неделям
    # используется при обновлении инфы в amo json week
    # возвращает список строк-сделок, которые были добавлены/обновлены
    # есть опасность, что итоговый список может получиться огромным, но 
    # пусть пока будет так. До рефакторинга.
    #  - основная функция при обновлении базы сделок из AmoCRM
        files_list = os.listdir(self.CONFIG['LEADS_TEMP_JSON_PATH'])
        result_json = {} #сюда собираем json с обновлёнными и добавленными сделками 
        for filename in files_list:    
            with open(self.CONFIG['LEADS_TEMP_JSON_PATH'] + filename, 'r', encoding="utf8") as json_file:
                deals = json.load(json_file)
            deals.sort(key=operator.itemgetter('created_at'))
            #Добавляем новые сделки в AMO JSON WEEK 
            result_json.append(self._merge_json_pack_to_json_week_deals(deals, week_json_path))
        rows = self.extract_amo_json_deals_to_rows(result_json)
        logger.info(f'Обновление AMO JSON WEEK завершено. Обновлено/добавлено {len(rows)} сделок')
        return rows
    
    #---- Внешние методы -----------
    # Скачать все сделки из AmoCRM
    def get_all_deals_from_crm(self):
        #self._get_all_deals_ext_to_json(self.CONFIG['LEADS_RAW_JSON_PATH'], self.CONFIG['JSON_DEALS_RAW_FILENAME'])
        self._put_deals_from_raw_json_to_week_json(self.CONFIG['LEADS_RAW_JSON_PATH'], self.CONFIG['LEADS_WEEK_JSON_PATH'], self.CONFIG['JSON_DEALS_WEEK_FILENAME'])
    
    # Получить датафрейм сделок созданных в определённую ISO-неделю
    def get_deals_dataframe_by_week(self, year, week_num):
        filename = self.CONFIG['JSON_DEALS_WEEK_FILENAME'] + '_' + str(year) + '_' + str(week_num).zfill(2) + '.json'
        with open(self.CONFIG['LEADS_WEEK_JSON_PATH'] + filename, 'r', encoding="utf8") as json_file:
            json_deals = json.load(json_file)
        df = self._extract_amo_json_deals_to_dataframe(json_deals)
        return df

    # Получить датафрейм сделок созданных в определённый год
    def get_deals_dataframe_by_year(self, year):
        files_list = os.listdir(self.CONFIG['LEADS_WEEK_JSON_PATH'])
        year_files_list = [file for file in files_list if ('_' + str(year) + '_') in file]
        rows = []
        for filename in year_files_list:
            with open(self.CONFIG['LEADS_WEEK_JSON_PATH'] + filename, 'r', encoding="utf8") as json_file:
                json_data = json.load(json_file)
            rows += self._extract_amo_json_deals_to_rows(json_data)
        df = pd.DataFrame(rows)
        return df
    
    ###=========================






if __name__ == "__main__":
        
    week_parser = AmoCRM()
    #week_parser._refresh_access_token()
    week_parser.get_all_deals_from_crm()
    sleep(100)




    rows = week_parser.extract_amo_json_file_to_rows(json_path, json_filename)
    df = pd.DataFrame(rows)
    logger.info(f'Загружен файл {json_filename}')
    logger.debug(f'Размер датафрейма: {df.shape} \n')

#    json_filename = os.path.join('/tests/', 'amo_json_2020_40.json')
    json_filename = 'amo_json_2020_31.json'
#    rows = week_parser.extract_amo_json_to_rows(dirname, json_filename)
    rows = week_parser.extract_amo_json_file_to_rows(json_path, json_filename)
    df = pd.DataFrame(rows)
    logger.debug(f'Загружен файл {json_filename}')
    logger.debug(f'Размер датафрейма: {df.shape} \n')

    rows = week_parser.extract_amo_json_from_directory_to_rows(json_path)
    df = pd.DataFrame(rows)
    logger.debug(f'Создан общий датафрейм: {df.shape} \n')
    print(list(df))
 #   print(df[['lead_utm_source', 'drupal_utm']])
 #   source_data = week_parser.extract(json_file_name)
 #   result_rows = week_parser.transform(source_data)
 #   week_parser.load(result_rows, tsv_file_name)


 