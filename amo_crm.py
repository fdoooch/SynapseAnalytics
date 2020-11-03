import os
import pandas as pd
import datetime as dt
import requests
import json
import operator
import dotenv
from time import sleep
from loguru import logger
#from pprint import pprint


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
AMO_LEADS_WEEK_JSON_PATH = 'amo_leads_week_json'
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

AMO_PIPELINES_ID = {"CNTX": 7038,
                    "WEB": 28752
                    }

#pipeline_id: trash_status_id
AMO_TRASH_STATUSES_ID = {7038: 28985871, #CNTX
                   28752: 29160522 #WEB
    }

#получение токена на время разработки
def amo_get_token_fdoooch():
    url = 'https://politsin.com/app/fdoooch'
    rs = requests.get(url)
    os.environ['AMO_ACCESS_TOKEN'] = json.loads(rs.text)['token']
    dotenv_file = dotenv.find_dotenv()
    dotenv.set_key(dotenv_file, 'AMO_ACCESS_TOKEN', json.loads(rs.text)['token'])
    dotenv.set_key(dotenv_file, 'AMO_REFRESH_TOKEN', json.loads(rs.text)['refreshToken'])


#NOT WORKING
#получаем токены Amo после установки интеграции
def amo_get_tokens():
    url = 'https://' + AMO_SUBDOMAIN + '.amocrm.ru/oauth2/access_token'
    headers = {
        "User-Agent": AMO_user_agent,
        "Content-Type": "application/json"
    }
    data = {
        "client_id": "1114de56-d014-4f0e-ae3a-a1437650a5e0",
        "client_secret": "DfQGnU6Jg7jY2IOUYNTZfmz30KoqmcjQrbXLik8x5IQ9QaqdULIA2ejppjFDkTbP",
        "grant_type": "authorization_code",
        "code": "def50200dd7fa09163d05fd27126c96b604049f78d862d92314677c31f8d3b1d93794f1dc60307f8adac0aa977a115646e304284909710aeb8e1de75b2c9907b0d86ebe87b7eaee5584818fb5936f35090781485ac519832c64ffbe80a63706312e420fb0dffc1ab3d3437ef730f5426738343b2be2a68c2de55c9b97f98e9a9e9c4ac8c3bdc44832b905f4c88f7a12b9641da53abe6dcddd3455bacb7dc91fe106087fafa1c6a7c35c27d108afe55e76ec5f66b08fe1d836d0538b6b1615256595208e5da828c64fa1b525b76d1b2fd39afc941f49e294a751e1b619f6b53f3c2eec182d8e153635bd6cbd97ef4eaddcbec3e22adc975ac015660ce6c61d734374158b5373ea680cccb7ca46b252e70fba195b3e64ed114d1976ce861ab7ca75b506a72a137b456a1bdc481440dd2c8d9179c866d897e4960f92c75de50ec94fedf5128fe13a2af17765006738fe75c38def7f7f411e5c9e690a25d44d49b3af455ea669b6e1b12631f97a8366c8629ca3084dd3ed39a6c42b17cd4a7e1b3700636e837ceeba442392f7fcb950e44e045c0b8041407860075ba85d27c605227e07ef8888e6c8f3367af500041af5f092c0dfedf581b83faa1cf1c1744e30a437d498161373022ff6c65276fcd19df99b0683cbbaed8d835cec7d5bb86610b7cb077d884da8c",
        "redirect_uri": AMO_REDIRECT_URI
        }
    try:
        rs = requests.post(url, headers=headers, data=data)
        logger.debug(json.loads(rs.text))

    except ConnectionError:
        logger.error('Ошибка ConnectionError ' + url)



  #NOT WORKING                
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
        logger.debug(rs.status_code)
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
                with open(AMO_LEADS_RAW_JSON_FOLDER_PATH + '/' + filename_amocrm_all_leads_ext_json.split('.')[0] + str(page) + '.' + filename_amocrm_all_leads_ext_json.split('.')[1], 'w', encoding="utf8") as output_file:
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


###============================================================================================================
###Упорядочивание архива json-файлов
###Перепаковываем сделки в файлы, группирующие их по неделе создания
###Название файлов: amo_leads_YYYY_WW.json
###Недели нумеруются по ISO
###первой неделей года считается неделя, содержащая первый четверг года, что эквивалентно следующим выражениям:
### неделя, содержащая 4 января;
### неделя, в которой 1 января это понедельник, вторник, среда или четверг;

#Обновляем сделку, в случае, если она уже присутствует в базе
def amo_update_deal_in_json_deals(new_deal, deals):
    #Если дата изменения соответствующей сделки в AMO JSON WEEK не меньше даты изменения new_deal - ничего не меняем
    deal_index = [old_deal['id'] for old_deal in deals].index(new_deal['id'])
    if deals[deal_index]['updated_at'] >= new_deal['updated_at']:
        logger.info(f'Сохранена имеющаяся информация по сделке #{new_deal["id"]}')
    
    #Если дата изменения новой сделки больше, а старая сделка содержит информацию о дате перехода в статус "Не целевой" (trash)
    # - заменяем старую сделку на новую, сохраная дату перехода в статус "Не целевой"
    elif 'trashed_at' in deals[deal_index]:
        new_deal['trashed_at'] = deals[deal_index]['trashed_at']
        deals.pop(deal_index)
        deals = amo_add_deal_to_json_deals(new_deal, deals)
        logger.info(f'Сделка #{new_deal["id"]} обновлена')
    
    #Иначе - просто заменяем старую сделку на новую
    else:
        deals.pop(deal_index)
        deals = amo_add_deal_to_json_deals(new_deal, deals)
        logger.info(f'Сделка #{new_deal["id"]} обновлена')

    return deals

#добавляем сделку в базу AMO JSON WEEK
def amo_add_deal_to_json_deals(new_deal, deals):
    #Если сделка в статусе "Треш - нецелевые", то добавляем дату последней модификации в поле ['trashed_at']
    if new_deal['status_id'] in AMO_TRASH_STATUSES_ID.values() and not 'trashed_at' in new_deal:
        new_deal['trashed_at'] = new_deal['updated_at']
    deals.append(new_deal)
    return deals

#Добавляем пакет сделок в нашу базу AMO JSON WEEK (Список сделок в JSON, разбитый на файлы по неделям создания сделки)
def amo_add_json_pack_to_json_week_deals(json_pack):
    #Если базы JSON WEEK нет - создаём
    if not os.path.exists(AMO_LEADS_WEEK_JSON_PATH):
        logger.info('База AMO JSON WEEK не найдена')
        os.mkdir(AMO_LEADS_WEEK_JSON_PATH)
        logger.info('Создана новая база AMO JSON WEEK в каталоге ' + AMO_LEADS_WEEK_JSON_PATH)

    count_added_deals = 0
    count_updated_deals = 0

    #Пока в пачке есть сделки добавляем их в базу
    while len(json_pack) > 0:
        new_deal = json_pack[0]
        year = dt.datetime.fromtimestamp(new_deal['created_at']).isocalendar()[0]
        week = dt.datetime.fromtimestamp(new_deal['created_at']).isocalendar()[1]
        
        #Если json с этой недели уже есть, дополняем его
        if os.path.isfile(AMO_LEADS_WEEK_JSON_PATH + '/' + 'amo_json_' + str(year) +'_'+ str(week).zfill(2) + '.json'):
            with open(AMO_LEADS_WEEK_JSON_PATH + '/' + 'amo_json_' + str(year) +'_'+ str(week).zfill(2) + '.json', 'r', encoding="utf8") as week_json_file:
                logger.info("Дополняем файл " + AMO_LEADS_WEEK_JSON_PATH + '/' + 'amo_json_' + str(year) +'_'+ str(week).zfill(2) + '.json',)
                week_deals = json.load(week_json_file)
                
                #Пока у новых сделок сохраняется номер недели
                while dt.datetime.fromtimestamp(json_pack[0]['created_at']).isocalendar()[0] == year and dt.datetime.fromtimestamp(json_pack[0]['created_at']).isocalendar()[1] == week:
                    #Если сделка с таким id уже содержится в базе JSON WEEK - обновляем её
                    if any(deal['id'] == json_pack[0]['id'] for deal in week_deals):
                        week_deals = amo_update_deal_in_json_deals(json_pack[0], week_deals)
                        logger.info(f'Сделка #{new_deal["id"]} обновлена')
                        count_updated_deals += 1
                    #если id новой сделки уникален - добавляем сделку
                    else:
                        week_deals = amo_add_deal_to_json_deals(json_pack[0], week_deals)
                        logger.info(f'Сделка #{json_pack[0]["id"]} добавлена в AMO JSON WEEK')
                        count_added_deals += 1
                    #Удаляем добавленную сделку из пачки
                    json_pack.pop(0)
                    #если сделок в пачке больше нет, сохраняем сделки в AMO JSON WEEK и завершаем функцию
                    if len(json_pack) == 0:
                        output_filename = 'amo_json_' + str(year) +'_'+ str(week).zfill(2) + '.json'
                        with open(AMO_LEADS_WEEK_JSON_PATH + '/' + output_filename, 'w', encoding="utf8") as output_file:
                            json.dump(week_deals, output_file, ensure_ascii=False, indent=2)
                        logger.info('Обновлён файл:' + output_filename)
                        logger.info('Добавление пачки сделок завершено')
                        logger.info(f'{count_added_deals} было добавлено')
                        logger.info(f'{count_updated_deals} было обновлено')
                        return

                #Если неделя в новой сделке отличаеся от той, с которой работали, то сохраняем сделки в AMO JSON WEEK и начинаем работу с новой неделей
                output_filename = 'amo_json_' + str(year) +'_'+ str(week).zfill(2) + '.json'
                with open(AMO_LEADS_WEEK_JSON_PATH + '/' + output_filename, 'w', encoding="utf8") as output_file:
                    json.dump(week_deals, output_file, ensure_ascii=False, indent=2)
                logger.info('Обновлён файл:' + output_filename)
                continue

        #Если в AMO JSON WEEK нет файла соответствующей недели, создаём его и наполняем
        else:
            logger.info(f'Добавляем в AMO JSON WEEK новую неделю: {week}, год: {year}')
            week_deals = []
            
            #Пока у новых сделок сохраняется номер недели
            while dt.datetime.fromtimestamp(json_pack[0]['created_at']).isocalendar()[0] == year and dt.datetime.fromtimestamp(json_pack[0]['created_at']).isocalendar()[1] == week:
                week_deals = amo_add_deal_to_json_deals(json_pack[0], week_deals)
                logger.info(f'Сделка #{json_pack[0]["id"]} добавлена в AMO JSON WEEK')
                count_added_deals += 1
                json_pack.pop(0)
                #если сделок в пачке больше нет, сохраняем сделки в AMO JSON WEEK и завершаем функцию
                if len(json_pack) == 0:
                    output_filename = 'amo_json_' + str(year) +'_'+ str(week).zfill(2) + '.json'
                    with open(AMO_LEADS_WEEK_JSON_PATH + '/' + output_filename, 'w', encoding="utf8") as output_file:
                        json.dump(week_deals, output_file, ensure_ascii=False, indent=2)
                    logger.info('Создан файл:' + output_filename)
                    logger.info('Добавление пачки сделок завершено')
                    logger.info(f'{count_added_deals} было добавлено')
                    logger.info(f'{count_updated_deals} было обновлено')
                    return


            #Если неделя в новой сделке отличаеся от той, с которой работали, то сохраняем сделки в AMO JSON WEEK и начинаем работу с новой неделей
            output_filename = 'amo_json_' + str(year) +'_'+ str(week).zfill(2) + '.json'
            with open(AMO_LEADS_WEEK_JSON_PATH + '/' + output_filename, 'w', encoding="utf8") as output_file:
                json.dump(week_deals, output_file, ensure_ascii=False, indent=2)
            logger.info('Обновлён файл:' + output_filename)
            continue              
    
    #Если в пачке кончились сделки - завершаем функцию
    return

#Разбираем данные по сделкам, скаченные из Амо в json и раскладываем их в файлы по неделям - основная функция
def amo_put_deals_from_raw_json_to_week_json():
    files_list = os.listdir(AMO_LEADS_RAW_JSON_FOLDER_PATH)
    for filename in files_list:    
        with open(AMO_LEADS_RAW_JSON_FOLDER_PATH + '/' + filename, 'r', encoding="utf8") as json_file:
            deals = json.load(json_file)
            deals.sort(key=operator.itemgetter('created_at'))
        logger.info(f'{filename} загружен в память - {len(deals)} сделок')
        #Добавляем новые сделки в AMO JSON WEEK
        amo_add_json_pack_to_json_week_deals(deals)
        logger.info('Обновление AMO JSON WEEK завершено')

###=============================
###========= Конец блока упорядочивания архива json-файлов
###=======================================================

###=============================================================
###Загрузка AMO JSON WEEK в датафреймы для дальнейшей работы

#получаем значение пользовательского поля из json сделки
#пользовательские поля лежат в разделе ['custom_fields_values']
def amo_get_custom_field_value_from_json_by_field_id(deal_json, field_id):
    lst = list(filter(lambda item:item['field_id']==field_id, deal_json['custom_fields_values']))
    try:
        result = dict(lst[0])['values'][0]['value']
    except KeyError:
        logger.debug('Параметр отсутствует в json')
        result = ""
    except IndexError:
        logger.debug(f'Поле {field_id} отсутствует в сделке #{deal_json["id"]}')
        result = ""
    return result

#Загружаем файл json из AMO JSON WEEK в датафрейм
def amo_get_dataframe_from_json_week(week_json_filename):
    #Загружаем json
    with open(AMO_LEADS_WEEK_JSON_PATH + '/' + week_json_filename, 'r', encoding="utf8") as json_file:
            json_deals = json.load(json_file)
    logger.info(f'{week_json_filename} загружен в память - {len(json_deals)} сделок')
    
    #создаём и заполняем датафрейм
    df_deals = pd.DataFrame()
    df_row_number = 0
    for deal in json_deals:
        logger.debug(f'Добавляю сделку #{deal["id"]} из файла {week_json_filename}')
        df_deals = df_deals.append({
            'id': str(deal['id']), #id сделки
            'created_at': str(deal['created_at']), #дата создания сделки
            'updated_at': str(deal['updated_at']), #дата последнего обновления сделки
            'amo_pipeline_id': str(deal['pipeline_id']), #id воронки в AmoCRM
            'amo_status_id': str(deal['status_id']) #id этапа в воронке AmoCRM
            }, ignore_index=True)
        
        #Добавляем значения пользовательских полей
        if deal['custom_fields_values'] != None:
            #город клиента
            df_deals.loc[df_row_number]['city'] = amo_get_custom_field_value_from_json_by_field_id(deal, 512318)
        
            #utm параметры из Друпала
            df_deals.loc[df_row_number]['drupal_utm'] = amo_get_custom_field_value_from_json_by_field_id(deal, 632884)
            #utm_source из Тильды
            df_deals.loc[df_row_number]['tilda_utm_source'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648158)
            #utm_medium из Тильды
            df_deals.loc[df_row_number]['tilda_utm_medium'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648160)
            #utm_campaign из Тильды
            df_deals.loc[df_row_number]['tilda_utm_campaign'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648310)
            #utm_content из Тильды
            df_deals.loc[df_row_number]['tilda_utm_content'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648312)
            #utm_term из Тильды
            df_deals.loc[df_row_number]['tilda_utm_term'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648314)
            #utm_source из CallTouch
            df_deals.loc[df_row_number]['ct_utm_source'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648256)
            #utm_medium из CallTouch
            df_deals.loc[df_row_number]['ct_utm_medium'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648258)
            #utm_campaign из CallTouch
            df_deals.loc[df_row_number]['ct_utm_campaign'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648260)
            #utm_content из CallTouch
            df_deals.loc[df_row_number]['ct_utm_content'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648262)
            #utm_term из CallTouch
            df_deals.loc[df_row_number]['ct_utm_term'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648264)

            #значения устаревшего поля Услуга
            df_deals.loc[df_row_number]['amo_products_2019'] = amo_get_custom_field_value_from_json_by_field_id(deal, 562024)
            #значения поля Услуга
            df_deals.loc[df_row_number]['amo_products_2020'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648028)
            #значение поля PRODUCT из тильды
            df_deals.loc[df_row_number]['tilda_products_2020'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648152)

            #piwik_id из Drupal
            df_deals.loc[df_row_number]['drupal_piwik_id'] = amo_get_custom_field_value_from_json_by_field_id(deal, 589816)
            #piwik_id из Tilda
            df_deals.loc[df_row_number]['tilda_piwik_id'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648530)
            #calltouch_session_id из Tilda
            df_deals.loc[df_row_number]['tilda_calltouch_session_id'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648532)
            #calltouch_session_id из CallTouch
            df_deals.loc[df_row_number]['ct_calltouch_session_id'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648288)
            #calltouch_client_id из CallTouch
            df_deals.loc[df_row_number]['ct_calltouch_client_id'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648290)
            #google_id из CallTouch
            df_deals.loc[df_row_number]['ct_google_id'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648292)
            #google_id из Drupal
            df_deals.loc[df_row_number]['drupal_google_id'] = amo_get_custom_field_value_from_json_by_field_id(deal, 589818)
            #yandex_id из CallTouch
            df_deals.loc[df_row_number]['ct_yandex_id'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648294)

            #cookies из Tilda
            df_deals.loc[df_row_number]['tilda_cookies'] = amo_get_custom_field_value_from_json_by_field_id(deal, 648166)

        #дата перевода в trashed
        if 'trashed_at' in deal:
            df_deals.loc[df_row_number]['trashed_at'] = deal['trashed_at']
        df_row_number += 1

    logger.info(f'Датафрейм создан - {len(json_deals)} сделок')
    logger.debug(df_deals.shape)

    return df_deals

#Создаём датафрейм по номеру недели и году
def amo_get_dataframe_from_json_week_by_week_number(year, week):
    week_json_filename = 'amo_json_' + str(year) + '_' + str(week).zfill(2) + '.json'
    logger.debug(f'Собираем датафрейм из {week_json_filename}')
    if os.path.isfile(AMO_LEADS_WEEK_JSON_PATH + '/' + week_json_filename):
        result = amo_get_dataframe_from_json_week(week_json_filename)
    else:
        result = pd.DataFrame()
    
    return result

#Создаём годовой датафрейм
def amo_get_dataframe_from_json_week_by_year_number(year):
    files = list(x for x in os.listdir(AMO_LEADS_WEEK_JSON_PATH) if ('amo_json_' + str(year) + '_') in x)
    #files = list(filter(lambda x: x.contains('amo_json_' + str(year) + '_'), os.listdir(AMO_LEADS_WEEK_JSON_PATH)))
    result_df = pd.DataFrame()
    for file in files:
        logger.debug(f'загружаем файл {file}')
        result_df = result_df.append(amo_get_dataframe_from_json_week(file), ignore_index=True)
    return result_df

#### Следующий шаг
#### Загрузить данные за неделю в датафрейм
#### Сохранить датафрейм в Google Data Sheet
#### Подгрузить новые сделки из Amo и добавить их в недельные сеты

