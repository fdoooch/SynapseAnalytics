#import yadirstat
import requests
import json
from requests.exceptions import ConnectionError
from time import time
from time import sleep
import config
import pprint

#  Метод для корректной обработки строк в кодировке UTF-8 как в Python 3, так и в Python 2
import sys
if sys.version_info < (3,):
    def u(x):
        try:
            return x.encode("utf8")
        except UnicodeDecodeError:
            return x
else:
    def u(x):
        if type(x) == type(b''):
            return x.decode('utf8')
        else:
            return x
# --- Входные данные ---
# Адрес сервиса AgencyClients для отправки JSON-запросов (регистрозависимый)
AgencyClientsURL = 'https://api-sandbox.direct.yandex.com/json/v5/agencyclients'

# Адрес сервиса Reports для отправки JSON-запросов (регистрозависимый)
ReportsURL = 'https://api-sandbox.direct.yandex.com/json/v5/reports'

# Адрес сервиса Balance
BalanceURL = 'https://api.direct.yandex.ru/live/v4/json'

# OAuth-токен представителя агентства, от имени которого будут выполняться запросы
token = config.YADIR_TOKEN


# Получаем список всех клиентов агентства
def get_agency_clients_list():
    # --- Подготовка запроса к сервису AgencyClients ---
    # Создание HTTP-заголовков запроса
    headers = {
            # OAuth-токен. Использование слова Bearer обязательно
            "Authorization": "Bearer " + token,
            # Язык ответных сообщений
            "Accept-Language": "ru"
            }

    AgencyClientsBody = {
        "method": "get",
        "params": {
            "SelectionCriteria": {
                "Archived": "NO"   # Получить только активных клиентов
            },
            "FieldNames": ["Login"],
            "Page": {
                "Limit": 10000,  # Получить не более 10000 клиентов в ответе сервера
                "Offset": 0
            }
        }
    }

    # --- Выполнение запросов к сервису AgencyClients ---
    # Отсутствие параметра LimitedBy в ответе означает, что
    # получены все клиенты
    HasAllClientLoginsReceived = False
    ClientList = []

    while not HasAllClientLoginsReceived:
        ClientsResult = requests.post(AgencyClientsURL, json.dumps(AgencyClientsBody), headers=headers).json()
        for Client in ClientsResult['result']['Clients']:
            ClientList.append(Client["Login"])
        if ClientsResult['result'].get("LimitedBy", False):
            AgencyClientsBody['Page']['Offset'] = ClientsResult['result']["LimitedBy"]
        else:
            HasAllClientLoginsReceived = True
    return ClientList


# Получаем список всех клиентов агентства
# и остатки на счетах
def get_clients_and_balance_list():
    # --- Подготовка запроса к сервису AgencyClients ---
    # Создание HTTP-заголовков запроса
    headers = {
            # OAuth-токен. Использование слова Bearer обязательно
            "Authorization": "Bearer " + token,
            # Язык ответных сообщений
            "Accept-Language": "ru"
            }

    AgencyClientsBody = {
        "method": "get",
        "params": {
            "SelectionCriteria": {
                "Archived": "NO"   # Получить только активных клиентов
            },
            "FieldNames": ["Login",],
            "Page": {
                "Limit": 10000,  # Получить не более 10000 клиентов в ответе сервера
                "Offset": 0
            }
        }
    }

    # --- Выполнение запросов к сервису AgencyClients ---
    # Отсутствие параметра LimitedBy в ответе означает, что
    # получены все клиенты
    HasAllClientLoginsReceived = False
    ClientList = []

    while not HasAllClientLoginsReceived:
        ClientsResult = requests.post(AgencyClientsURL, json.dumps(AgencyClientsBody), headers=headers).json()
        for Client in ClientsResult['result']['Clients']:
            ClientList.append(Client["Login"])
        if ClientsResult['result'].get("LimitedBy", False):
            AgencyClientsBody['Page']['Offset'] = ClientsResult['result']["LimitedBy"]
        else:
            HasAllClientLoginsReceived = True
    
    
    ClientsBalanceBody = {
        "method": "AccountManagement",
        "token": token,
        "params": {
            "Action": "Get",
            "SelectionCriteria": {
                "Logins": "synapse-studio"   # Список логинов для получения баланса
            },
            "Page": {
                "Limit": 10000,  # Получить не более 10000 клиентов в ответе сервера
                "Offset": 0
            }
        }
    }
    pp = pprint.PrettyPrinter(indent=4)
    BalanceResult = requests.post(BalanceURL, json.dumps(ClientsBalanceBody), headers=headers).json()
    pp.pprint(BalanceResult)
    print('finita')        
            
    return ClientList

#Пример от яндекса - получение сводного отчёта по списку клиентов
def demo_get_report_for_clients_list(clients_list):
    # --- Подготовка запроса к сервису Reports ---
    # Создание тела запроса
    # Отчет содержит количество показов, кликов и расход средств по всем кампаниям клиента
    body = {
        "params": {
            "SelectionCriteria": {},
            "FieldNames": [
                "Impressions",
                "Clicks",
                "Cost"
            ],
            "ReportName": u("ACCOUNT_PERFORMANCE"),
            "ReportType": "ACCOUNT_PERFORMANCE_REPORT",
            "DateRangeType": "AUTO",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "NO"
        }
    }

    # Создание результирующих данных
    resultcsv = "Login;Impressions;Clicks;Costs\n"

    # Дополнительные HTTP-заголовки для запроса отчетов
    headers['skipReportHeader'] = "true"
    headers['skipColumnHeader'] = "true"
    headers['skipReportSummary'] = "true"
    headers['returnMoneyInMicros'] = "false"

    # --- Выполнение запросов к сервису Reports ---
    for client in clients_list:
        # Добавление HTTP-заголовка "Client-Login"
        headers['Client-Login'] = client
        # Кодирование тела запроса в JSON
        requestBody = json.dumps(body, indent=4)
        # Запуск цикла для выполнения запросов
        # Если получен HTTP-код 200, то содержание отчета добавляется к результирующим данным
        # Если получен HTTP-код 201 или 202, выполняются повторные запросы
        while True:
            try:
                req = requests.post(ReportsURL, requestBody, headers=headers)
                req.encoding = 'utf-8'  # Принудительная обработка ответа в кодировке UTF-8
                if req.status_code == 400:
                    print("Параметры запроса указаны неверно или достугнут лимит отчетов в очереди")
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код запроса: {}".format(u(body)))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                elif req.status_code == 200:
                    print("Отчет для аккаунта {} создан успешно".format(str(Client)))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    if req.text != "":
                        tempresult = req.text.split('\t')
                        resultcsv += "{};{};{};{}\n".format(Client, tempresult[0], tempresult[1], str(tempresult[2]).replace('.', ','))
                    else:
                        resultcsv += "{};0;0;0\n".format(Client)
                    break
                elif req.status_code == 201:
                    print("Отчет для аккаунта {} успешно поставлен в очередь в режиме offline".format(str(Client)))
                    retryIn = int(req.headers.get("retryIn", 60))
                    print("Повторная отправка запроса через {} секунд".format(retryIn))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    sleep(retryIn)
                elif req.status_code == 202:
                    print("Отчет формируется в режиме офлайн".format(str(Client)))
                    retryIn = int(req.headers.get("retryIn", 60))
                    print("Повторная отправка запроса через {} секунд".format(retryIn))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    sleep(retryIn)
                elif req.status_code == 500:
                    print("При формировании отчета произошла ошибка. Пожалуйста, попробуйте повторить запрос позднее.")
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                elif req.status_code == 502:
                    print("Время формирования отчета превысило серверное ограничение.")
                    print(
                        "Пожалуйста, попробуйте изменить параметры запроса - уменьшить период и количество запрашиваемых данных.")
                    print("JSON-код запроса: {}".format(body))
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break
                else:
                    print("Произошла непредвиденная ошибка")
                    print("RequestId: {}".format(req.headers.get("RequestId", False)))
                    print("JSON-код запроса: {}".format(body))
                    print("JSON-код ответа сервера: \n{}".format(u(req.json())))
                    break

            # Обработка ошибки, если не удалось соединиться с сервером API Директа
            except ConnectionError:
                # В данном случае мы рекомендуем повторить запрос позднее
                print("Произошла ошибка соединения с сервером API")
                # Принудительный выход из цикла
                break

            # Если возникла какая-либо другая ошибка
            except:
                # В данном случае мы рекомендуем проанилизировать действия приложения
                print("Произошла непредвиденная ошибка")
                # Принудительный выход из цикла
                break

    print("Создание отчетов для аккаунтов завершено")

    # Создание и запись файла
    filename = "AccountsPerfomanceReport_{}.csv".format(str(time()))
    resultfile = open(filename, 'w+')
    resultfile.write(resultcsv)
    resultfile.close()

    print("Результат записан в файл {}".format(filename))




########################
########################
########################
clients_list = get_clients_and_balance_list()
for client in clients_list:
    print(client)