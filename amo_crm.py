import os
import pandas as pd
import datetime as dt
import requests
import json
import dotenv
from time import sleep
from loguru import logger


filename_result = 'amocrm_stat_tsv'
filename_amocrm = 'amocrm_raw_cntx_tsv.tsv'
filename_amocrm_all_leads_ext_json = 'amocrm_all_leads_ext_json.json'
AMO_REDIRECT_URI = 'https://hook.integromat.com/78sigwp948jnsjf2ndodfctwc3yuechm'

AMO_access_token = os.getenv("AMO_ACCESS_TOKEN")
AMO_page_shift = int(os.getenv("AMO_PAGE_SHIFT"))
AMO_user_agent = 'amoCRM-oAuth-client/1.0'
AMO_content_type = 'application/json'
AMO_SUBDOMAIN = 'syn'
AMO_LEADS_RAW_JSON_FOLDER_PATH = "amo_leads_raw_json"
AMO_PAUSE_BETWIN_REQUESTS = 4
AMO_PAGE_SIZE = 250
AMO_PAGES_COUNT_PER_LOAD = 50 #Количество страниц размером AMO_PAGE_SIZE, которое будем подгружать за один запуск скрипта (для случаем, когда нам нужно выгрузить из AMO много сделок)

#Для начала скачиваем все сделки со ссылками на контакты из Amo и сохраняем их в набор JSON-файлов в папку amo_leads_raw_json
#Делаем это с помощью функции amo_get_all_deals_ext_to_json()

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

#получаем токены Amo после установки интеграции
def amo_get_tokens():
    url = 'https://' + AMO_SUBDOMAIN + '.amocrm.ru/oauth2/access_token'
    headers = {
        "User-Agent": AMO_user_agent,
        "Content-Type": "application/json"
    }
    data = {
        "client_id": "1114de56-d014-4f0e-ae3a-a1437650a5e0",
        "client_secret": "DrwoqgcW2DD3kzUEHf5AjsGNMTQABWkgkSkeg0XXud4TjHJ4SrViXo4Qt1b5T1HQ",
        "grant_type": "authorization_code",
        "code": "def50200dd7fa09163d05fd27126c96b604049f78d862d92314677c31f8d3b1d93794f1dc60307f8adac0aa977a115646e304284909710aeb8e1de75b2c9907b0d86ebe87b7eaee5584818fb5936f35090781485ac519832c64ffbe80a63706312e420fb0dffc1ab3d3437ef730f5426738343b2be2a68c2de55c9b97f98e9a9e9c4ac8c3bdc44832b905f4c88f7a12b9641da53abe6dcddd3455bacb7dc91fe106087fafa1c6a7c35c27d108afe55e76ec5f66b08fe1d836d0538b6b1615256595208e5da828c64fa1b525b76d1b2fd39afc941f49e294a751e1b619f6b53f3c2eec182d8e153635bd6cbd97ef4eaddcbec3e22adc975ac015660ce6c61d734374158b5373ea680cccb7ca46b252e70fba195b3e64ed114d1976ce861ab7ca75b506a72a137b456a1bdc481440dd2c8d9179c866d897e4960f92c75de50ec94fedf5128fe13a2af17765006738fe75c38def7f7f411e5c9e690a25d44d49b3af455ea669b6e1b12631f97a8366c8629ca3084dd3ed39a6c42b17cd4a7e1b3700636e837ceeba442392f7fcb950e44e045c0b8041407860075ba85d27c605227e07ef8888e6c8f3367af500041af5f092c0dfedf581b83faa1cf1c1744e30a437d498161373022ff6c65276fcd19df99b0683cbbaed8d835cec7d5bb86610b7cb077d884da8c",
        "redirect_uri": AMO_REDIRECT_URI
        }
    try:
        rs = requests.post(url, headers=headers, data=data)
        logger.debug(json.loads(rs.text))

    except ConnectionError:
        logger.error('Ошибка ConnectionError ' + url)



                  
#обновляем access-токен
def amo_refresh_access_token():
    url = 'https://' + AMO_SUBDOMAIN + '.amocrm.ru/oauth2/access_token'
    headers = {
       "User-Agent": AMO_user_agent,
       "Content-Type": "application/json"
    }
    data = {"client_id": os.getenv("AMO_CLIENT_ID"),
       "client_secret": os.getenv("AMO_CLIENT_SECRET"),
       "grant_type": "refresh_token",
       "refresh_token": os.getenv("AMO_REFRESH_TOKEN"),
       "redirect_uri": AMO_REDIRECT_URI
       }
    try:
        rs = requests.post(url, headers=headers, json=data)
        dotenv_file = dotenv.find_dotenv()
        dotenv.set_key(dotenv_file, 'AMO_ACCESS_TOKEN', json.loads(rs.text)['access_token'])
        dotenv.set_key(dotenv_file, 'AMO_REFRESH_TOKEN', json.loads(rs.text)['refresh_token'])
        os.environ['AMO_ACCESS_TOKEN'] = json.loads(rs.text)['access_token']
        os.environ['AMO_REFRESH_TOKEN'] = json.loads(rs.text)['refresh_token']
        logger.info('AmoCRM tokens wad refreshed')

    except ConnectionError:
        logger.error('Ошибка ConnectionError ' + url)
    
    

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
#Отсортирован по возрастанию даты создания
#Page - номер страницы (размер страницы = limit)
#Код ответа 204 означает, что контента больше нет
def amo_get_deals_sorted_by_created_date_ext(limit, page):
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
        "Authorization": 'Bearer ' + os.getenv("AMO_ACCESS_TOKEN")
    }

    params = {
        "limit": limit,
        "page": page,
        "with": "catalog_elements,contacts,loss_reason",
        "order[created_at]": "inc"
    }

    try:
        rs = requests.get(url, headers=headers, params=params)
        return rs

    except ConnectionError:
        logger.error('Ошибка ConnectionError ' + url)


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
        logger.error('Ошибка ConnectionError ' + url)


#Выкачиваем все сделки со списками из AMO отсортированные по дате последней модификации по убыванию и сохраняем их в JSON-файл
#Эту функцию используем тогда, когда у нас на сервере нет никакого списка сделок
@logger.catch
def amo_get_all_deals_ext_to_json():
    logger.info('Скачиваем все сделки в расширенном формате. PAGE_SHIFT = ' + str(AMO_page_shift))
    page = 1
    limit = AMO_PAGE_SIZE
    os.environ['AMO_PAGE_SHIFT'] = str(AMO_page_shift + 1)
    if not os.path.isdir(AMO_LEADS_RAW_JSON_FOLDER_PATH):
        os.mkdir(AMO_LEADS_RAW_JSON_FOLDER_PATH)
    all_leads = []
    has_more = True
    while has_more:
        rs = amo_get_deals_sorted_by_created_date_ext(limit, page)
        if rs.status_code == 200:
            json_string = json.loads(rs.text)
            all_leads += (json.loads(rs.text))['_embedded']['leads']
            #Если количество полученных записей достигла предела - сохраняем их в файл и обнуляем массив
            if page % AMO_PAGES_COUNT_PER_LOAD == 0:
                with open(AMO_LEADS_RAW_JSON_FOLDER_PATH + '/' + filename_amocrm_all_leads_ext_json + str(page), 'w', encoding="utf8") as output_file:
                    json.dump(all_leads, output_file, ensure_ascii=False)
                all_leads = []
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
        sleep(AMO_PAUSE_BETWIN_REQUESTS)
        logger.info(f'Page: {page}')
    with open(AMO_LEADS_RAW_JSON_FOLDER_PATH + '/' + filename_amocrm_all_leads_ext_json + str(page), 'w', encoding="utf8") as output_file:
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


