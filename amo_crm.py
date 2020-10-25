import os
import pandas as pd
import datetime as dt
import requests
import json
from time import sleep


filename_result = 'amocrm_stat_tsv'
filename_amocrm = 'amocrm_raw_cntx_tsv.tsv'
filename_amocrm_all_leads_ext_json = 'amocrm_all_leads_уч_json.json'
AMO_access_token = os.getenv("AMO_ACCESS_TOKEN")
AMO_user_agent = 'amoCRM-oAuth-client/1.0'
AMO_content_type = 'application/json'
AMO_SUBDOMAIN = 'syn'
AMO_PAUSE_BETWIN_REQUESTS = 4
AMO_PAGE_SIZE = 250


AMO_RAW_FIELDS = {'items': 648028,
                  'product_tilda': 648152,
                  'utm_source_tilda': 648158,
                  'utm_medium_tilda': 648160,
                  'utm_campaign_tilda': 648310,
                  'utm_term_tilda': 648314,
                  'form_id_tilda': 648164,
                  'form_name_tilda': 648162,
                  'cookies_tilda': 648166,
                  'referer_tilda': 648168,
                  'page_tilda':648556,
                  'city_tilda':648980,
                  'chanel_amo': 600935,
                  'city_amo': 512318,
                  'create_from_ct': 648218,
                  'type_communication_ct': 648220,
                  'client_phone_number_ct': 648224,
                  'communication_date_ct': 648232,
                  'communication_time_ct': 648234,
                  'call_long_ct': 648236,
                  'call_waiting_ct': 648238,
                  'utm_source_ct': 648256,
                  'utm_medium_ct': 648258,
                  'utm_campaign_ct': 648260,
                  'utm_content_ct': 648262,
                  'utm_term_ct': 648264,
                  'referer_ct': 648266,
                  'page_ct': 648268,
                  'city_ct': 648274,
                  'device_ct': 648276,
                  'os_ct': 648278,
                  'browser_ct': 648280,
                  'call_id_ct': 648282,
                  'piwik_id_drupal': 589816,
                  'google_id_drupal': 589818,
                  'page_drupal': 587868,
                  'utm_drupal': 632884,
                  'old_items': 562024                   
                  }

#Получаем информацию о субдомене, с которым работаем - пока просто для проверки работоспособности кода
def amo_check_domain():
    url = 'https://' + AMO_SUBDOMAIN + '.amocrm.ru/api/v2/account'
    headers = {
        "User-Agent": AMO_user_agent,
        "Authorization": 'Bearer ' + AMO_access_token
    }

    errors = {
        400: 'Bad request',
        401: 'Unauthorized',
        403: 'Forbidden',
        404: 'Not found',
        500: 'Internal server error',
        502: 'Bad gateway',
        503: 'Service unavailable'
    }

    try:
        rs = requests.get(url, headers=headers)
        if rs.status_code != 200:
            print("Ошибка! URL: {0}, код ошибки: {1} - {2}".format(url, rs.status_code, errors[rs.status_code]))
        else:
            print(rs.json())

    except ConnectionError:
        print('Ошибка ConnectionError', url)


#Получаем список сделок с элементами списков. limit = 0 .. 500, но лучше 50
#Отсортирован по убыванию даты последней модификации
#Page - номер страницы (размер страницы = limit)
#Код ответа 204 означает, что контента больше нет
def amo_get_deals_ext(limit, page):
    url = 'https://' + AMO_SUBDOMAIN + '.amocrm.ru/api/v4/leads'
    errors = {
        400: 'Bad request',
        401: 'Unauthorized',
        403: 'Forbidden',
        404: 'Not found',
        500: 'Internal server error',
        502: 'Bad gateway',
        503: 'Service unavailable'
    }
    
    headers = {
        "User-Agent": AMO_user_agent,
        "Authorization": 'Bearer ' + AMO_access_token
    }

    params = {
        "limit": limit,
        "page": page,
        "with": "catalog_elements,contacts,loss_reason",
        "order[updated_at]": "desc"
    }

    try:
        rs = requests.get(url, headers=headers, params=params)
        return rs

    except ConnectionError:
        return 'Ошибка ConnectionError ' + url

#Выкачиваем все сделки со списками из AMO отсортированные по дате последней модификации по убыванию и сохраняем их в JSON-файл
#Эту функцию используем тогда, когда у нас на сервере нет никакого списка сделок
def amo_get_all_deals_ext_to_json():
    print('качаем все сделки!')
    page = 1
    limit = AMO_PAGE_SIZE
    all_leads = []
    has_more = True
    while has_more:
        rs = amo_get_deals_ext(limit, page)
        if rs.status_code == 200:
            json_string = json.loads(rs.text)
            all_leads += (json.loads(rs.text))['_embedded']['leads']
        elif rs.status_code == 204:
            print('загрузка успешно завершена')
            has_more = False
            break
        else:
            print(f'Ошибка {rs.status_code}')
            has_more = False
            break
        page += 1
        sleep(AMO_PAUSE_BETWIN_REQUESTS)
        print(f'Page: {page}')
    with open(filename_amocrm_all_leads_ext_json, 'w', encoding="utf8") as output_file:
        json.dump(all_leads, output_file, ensure_ascii=False)

    return 0

#Получаем данные со списками по последним изменённым сделкам
def amo_get_last_modified_deals_ext(limit, page):
    url = 'https://' + AMO_SUBDOMAIN + '.amocrm.ru/api/v4/leads'
    errors = {
        400: 'Bad request',
        401: 'Unauthorized',
        403: 'Forbidden',
        404: 'Not found',
        500: 'Internal server error',
        502: 'Bad gateway',
        503: 'Service unavailable'
    }
    
    headers = {
        "User-Agent": AMO_user_agent,
        "Authorization": 'Bearer ' + AMO_access_token
    }

    params = {
        "limit": limit,
        "page": page,
        "with": "catalog_elements,contacts",
        "order[updated_at]": "desc"
    }

    try:
        rs = requests.get(url, headers=headers, params=params)
        return rs

    except ConnectionError:
        return 'Ошибка ConnectionError ' + url


#API AmoCRM: Получаем заданное количество последних изменённых сделок в формате json
def amo_get_some_last_modified_deals_ext(count):
    result = []
    print(f'Запрошено сделок: {count}')
    if count//AMO_PAGE_SIZE > 0:
        for i in range(1, count//AMO_PAGE_SIZE + 1):
            rs = amo_get_last_modified_deals_ext(AMO_PAGE_SIZE, i)
            if rs.status_code == 200:
                json_string = json.loads(rs.text)
                result += (json.loads(rs.text))['_embedded']['leads']
            elif rs.status_code == 204:
                print('запрошенное количество отсутствует')
                break
            else:
                print(f'Ошибка {rs.status_code} - ')
                break
            sleep(AMO_PAUSE_BETWIN_REQUESTS)
    if count%AMO_PAGE_SIZE > 0:
        rs = amo_get_last_modified_deals_ext(count%AMO_PAGE_SIZE, 1)
        if rs.status_code == 200:
            json_string = json.loads(rs.text)
            result += (json.loads(rs.text))['_embedded']['leads']
        elif rs.status_code == 204:
            print('запрошенное количество отсутствует')
        else:
            print(f'Ошибка {rs.status_code}')
    return result

#Актуализируем наш json-файл
def amo_sync():
    with open(filename_amocrm_all_leads_ext_json, encoding="utf8") as json_file:
        data = json.load(json_file)
    print('Самая поздняя сделка сейчас:', data[-1]['updated_at'])
    rs = amo_get_last_modified_deals_ext(AMO_PAGE_SIZE, 1)
    if rs.status_code == 200:
        new_data = (json.loads(rs.text))['_embedded']['leads']
        print('\nСкачали 1 пачку, самая ранняя сделка:', new_data[-1]['updated_at'])
        print('Самая поздняя сделка:', new_data[0]['updated_at'])
    elif rs.status_code == 204:
        print('запрошенное количество отсутствует')
    else:
        print(f'Ошибка {rs.status_code} - ')
    page = 1
    while new_data[-1]['updated_at'] > data[-1]['updated_at']:
        page += 1
        rs = amo_get_last_modified_deals_ext(AMO_PAGE_SIZE, page)
        if rs.status_code == 200:
            print('\n')
            print(f'Скачали {page} пачку')
            print('Cамая ранняя сделка:', (json.loads(rs.text))['_embedded']['leads'][-1]['updated_at'])
            print('Cамая поздняя сделка:', (json.loads(rs.text))['_embedded']['leads'][0]['updated_at'])
            new_data += (json.loads(rs.text))['_embedded']['leads']
        elif rs.status_code == 204:
            print('запрошенное количество отсутствует')
        else:
            print(f'Ошибка {rs.status_code} - ')
    while new_data[-1]['updated_at'] < data[-1]['updated_at']:
        new_data.pop()
    print(f'Обновляем {len(new_data)} сделок')
#    Теперь нужно добавить сделки, отсутствующие в файле
#    и обновить сделки присутствующие в нём и в новом списке (для этого логично написать функцию обновления сделки. Хочу сохранять дату перевода сделки в треш в json)



 #   sleep(AMO_PAUSE_BETWIN_REQUESTS)
 


#Получаем инфу по сделке с заданным ID из файла json
def amo_get_deal_from_json_by_id(json_filename, deal_id):
    with open(json_filename, encoding="utf8") as json_file:
        data = json.load(json_file)
        print('загрузка завершена')
        return data[[lead['id'] for lead in data].index(deal_id)]

def amo_get_last_deal_from_json(json_deals):
    print(json_deals[-1])
    print('\n', json_deals[-2])


def amocrm_utm_prepare(df):
    df['lead_source'] = df['UTM_SOURCE'].str.lower()
    df['lead_medium'] = df['UTM_MEDIUM'].str.lower()
    df.loc[df['Utm-метка'].fillna('').str.contains('source=yandex,|medium=yandex,', case=False, regex=True), 'lead_source'] = 'yandex'
    df.loc[df['Utm-метка'].fillna('').str.contains('source=google,|medium=google,', case=False, regex=True), 'lead_source'] = 'google'
    df.loc[df['Utm-метка'].fillna('').str.contains('source=search,|medium=search,', case=False, regex=True), 'lead_medium'] = 'search'
    df.loc[df['Utm-метка'].fillna('').str.contains('source=context,|source=context\-cpc,|medium=context,|medium=context\-cpc,', case=False, regex=True), 'lead_medium'] = 'context'
    df.loc[df['Utm-метка'].fillna('').str.contains('source=re,|medium=re,', case=False, regex=True), 'lead_medium'] = 're'


   #f.loc[df['Utm-метка'].str.lower().split('source=')[1].split(',')[0] == 'yandex', 'lead_source']
   # df['lead_medium'] = df[['Utm-метка']][~df['Utm-метка'].isnull() and df['lead_medium'].isnull()].str
   # df['lead_source'][~df['Utm-метка'].isnull() and df['lead_source'].isnull()] = df['Utm-метка']
    return df




#Подготовка файла из AmoCRM
def amocrm_prepare_stat(df):
    df['date'] = pd.to_datetime(df['Дата создания'], format='%d.%m.%Y %H:%M:%S')
    df = df[['ID', 'date', 'Город', 'Город CallTouch', 'Услуга', 'Этап сделки', 'Канал', 'Utm-метка', 'ID piwik', 'UTM_SOURCE', 'UTM_MEDIUM', 'Источник / utm_source', 'Канал / utm_medium']]
    df['week'] = df['date'].dt.isocalendar().week
    df['month'] = df['date'].dt.month
    df = df.rename({'Город': 'city', 'id': 'lead_id', 'Этап сделки': 'funnel_stage', 'Канал': 'channel'}, axis=1)
    df['city'] = df['city'].str.lower()
    df['funnel_stage'] = df['funnel_stage'].str.lower()
    df['channel'] = df['channel'].str.lower()
    df = amocrm_utm_prepare(df)
    return df

def amocrm_dataframe_preparation():
    df_amo = pd.read_csv(filename_amocrm, header=0, sep='\t', decimal=',')
    df_amo = amocrm_prepare_stat(df_amo)
    return df_amo


