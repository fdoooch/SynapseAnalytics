import pandas as pd
import datetime as dt
import requests
import json
from time import sleep


filename_result = 'amocrm_stat_tsv'
filename_amocrm = 'amocrm_raw_cntx_tsv.tsv'
filename_amocrm_all_leads_json = 'amocrm_all_leads_json.json'
AMO_access_token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjlkNGI5MmQ3NGZmODY5ZWNhN2M3Nzg4NzkzZjA2Yzc4MTNlNTEwNjQ0MDBkZTAwMDQzZjVhYjM1Nzc2ZDk1NTQyM2RlYWEzN2E0NTBlNWM0In0.eyJhdWQiOiIxYmEyNDUxNi1mZGQ1LTRlMTQtYWE4MS00NDc2YTQ1ZGFlZWUiLCJqdGkiOiI5ZDRiOTJkNzRmZjg2OWVjYTdjNzc4ODc5M2YwNmM3ODEzZTUxMDY0NDAwZGUwMDA0M2Y1YWIzNTc3NmQ5NTU0MjNkZWFhMzdhNDUwZTVjNCIsImlhdCI6MTYwMzM5OTEwOCwibmJmIjoxNjAzMzk5MTA4LCJleHAiOjE2MDM0ODU1MDgsInN1YiI6IjIxNjEyNCIsImFjY291bnRfaWQiOjkyMjExMDksInNjb3BlcyI6WyJwdXNoX25vdGlmaWNhdGlvbnMiLCJjcm0iLCJub3RpZmljYXRpb25zIl19.LRVG6h4lo8YGA4iFrzn31P82B9CuFv4UdrAZLrrebmaz411wPaZQCBkHajCRpB1i84mQ6KhmMT1mS8A1h7SfUEp8_3Hsw60iJSH5AaO6lmrgbC8bltOBaOGNKdxeglk4iwQvKnEwXv85WDY184IrMF8bkZ_0f6Z-wfHu6iRMubOMfQcDKuxwRFUnI34CpW99HtZY2mg6czrcb6E-_yrVRTZQ8VjQ0_AoOJCMBs6bw6ibMSnj2_I_NX0XEc-Nt1muLauTixmYFfzvZCd6xBcVkiO75_YjKzLFMASOOOfYadLybpqjOmnjmZTywzXWcyNzxg3_mku7qxz_Otxqrdl6hA'
AMO_user_agent = 'amoCRM-oAuth-client/1.0'
AMO_SUBDOMAIN = 'syn'
AMO_PAUSE_BETWIN_REQUESTS = 15
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






def AmoCRMOAuth():
    secret_key = 'kyP0zRZzyGt8sU88jreMu3ERDBpjdUN0fTQ0iWPbxCTtRuHPyBFtfdgN5FjAS2Wv'
    integration_id = '1114de56-d014-4f0e-ae3a-a1437650a5e0'
    authorisation_code = 'def502002deaa329c855f2c05362d9d99d80f0e0bde1e9ea467bdb23e97282fe6489ef8e9b78f8ff296e0a6a150d0d1cce1f9fac03ece3b3f9d026ff0e7c778dd3aadc6eb7beefd320355a68bae7028bc184d8fce8a38588a76250a7ac541262649d918f7915962b05c92e5ea2ba0273cc7811c3c1ac8440b72a6474724cd4a4a23b0edd5cc9f0bcfeed310e6fc90f3839d7e72309da5a8c7bd24ab0b9e59693bdfa875e989c6e4743316d41af0d8e70776477f7e10b4345f9d97eeef2f32c5fc46ffa10b1e989f0fc47415ee36b2d5cf35ca974965fb8c8a42d7610f5a44f4042c224b2774d7cb3ab9ee1a42632c4570ad0ae1dc3beda1c4e7af64e382e441dae745908a23f77a372222342194693d3b5a85e75a99bcf514085c673098cf7bd006db90f7c49b8f57552cf1b883edff456845e65ebfede8eed1f1f99f88627988679abd19a548496b4c2676a84e5b1e81a595355a0bb2564b318c4d36da417b87425ff7439d0117981021847003cd6df88c4005d742604f91581e39857578a67150b547daa07720148b26ec95d25f3d3932ec996337b3afc7560b82989e5eb18889e8e7f578085484ea22fe0b060f62646c7ad5b7feda979fed0458042806d4ed6b4'
    AMO_access_token_URL = 'https://syn.amocrm.ru/oauth2/access_token'
    AMO_refresh_token = 'def50200ba6d36a9fe94db8ea01ee4678cd9e5959e21766ccbc4fae00ac178089543f11cc76bccf0f56ad7e5d8ee1dcbd9ed90e54ab8b09b4c213bcf8bee15791b687208be222920c5216d8a8abe7ac9f07f792bd129d268b6aa61f16ea72ffdf7d079315d8497651481e1ae95155fbcf30359ee286190ad8cc7162d29f3c31f2e886bd4cc2282d7dc63b3f03888cc99bd56f29d4e4f75a617550db038f428c18fd3d66f71f8c40d0b40d3b0e0eaf632b583abb5f6180f33ad730d1fc823d34da1175ee7c2dc1eb27bc3d302fbbb4eebe9f54dc8549032254cc4305af3ae2c95d3368e0299aacb48e58ff0bf98dd20ef3ecc9a30badc12f20ec130d4c358ef46e2f7b6a22dd81f19167c354848f7708ff4ce8ba0253838710c8f128208636db9292097d9587cb1d19c80f55f46254775edf9510dcceff81ce37525ac2bf6f4613d61161eb3110049616c44ce3215f8aa7e6bdc3d91fde83c506620f3c75c9cee3cf6df1957e4e67f77835e25cb85c71acdb8fccfebb3630b22c7b73b665feb6382c4547bf86f0e6d59003c6130dc7d78ae8841bd0e379c2b8e9c022f62437465b0673b2eb916e08b699f047900a7c31fded26f4194bc17921f364298e619272a5461f0c09c1c320ee8'
    
    AMO_content_type = 'application/json'

    return 0

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
        "with": "catalog_elements",
        "order[updated_at]": "asc"
    }

    try:
        rs = requests.get(url, headers=headers, params=params)
        return rs

    except ConnectionError:
        return 'Ошибка ConnectionError ' + url

#Выкачиваем все сделки со списками из AMO отсортированные по дате последней модификации по возрастанию и сохраняем их в JSON-файл
def amo_get_all_deals_ext_to_json():
    page = 1
    limit = AMO_PAGE_SIZE
    all_leads = []
    has_more = True
    while has_more:
        rs = amo_get_deals_ext(limit, page)
        if rs.status_code == 200:
            json_string = json.loads(rs.text)
           # print(json_string['_embedded']['leads'])
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
    with open(filename_amocrm_all_leads_json, 'w', encoding="utf8") as output_file:
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
    with open(filename_amocrm_all_leads_json, encoding="utf8") as json_file:
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


