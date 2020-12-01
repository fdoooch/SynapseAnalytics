#update_amo_rows_deal.py
#Скрипт забирает дату последнего обновления из базы BigQuery amo_deals_raw
#скачивает из AmoCRM сделки обновлённые после этой даты
#сохраняет эти сделки в AMO DEALS JSON WEEK
#добавляет сделки в BigQuery amo_deals_raw
#предназначен для регулярного (ежедневного) запуска ночью, для актуализации данных по сделкам Amo
import google_bigquery as gb
import amo_crm as amo
import os
import config
import pandas as pd
from loguru import logger

#Получаем дату последнего обновления сделок в BigQuery
def get_deals_last_update_timestamp():
    result = gb.get_max_field(config.BQ_DS_MAIN, config.BQ_TB_AMO_DEALS_RAW, 'updated_at')
    return result

#Выкачиваем из AmoCRM обновившиеся сделки
def get_updated_deals_since_timestamp_to_dataframe(last_update):
    amo.amo_get_token_fdoooch()
    amo.get_updated_deals_since_timestamp_to_json_temp_folder(last_update)
    #Раскладываем скаченные сделки по неделям и собираем датафрейм для передачи в BigQuery
    df = amo.update_amo_json_week()

    return df


last_update_timestamp = get_deals_last_update_timestamp()

df_updated_deals = get_updated_deals_since_timestamp_to_dataframe(last_update)

###Ближайший шаг - реализовать создание датафрейма при обновлении базы AMO JSON WEEK Deals