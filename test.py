import os
import config
import google_bigquery as bq
import json
import amo_crm as amo
from heapq import merge
import pandas as pd
import AmoCRM
import GoogleBQ_deals
from loguru import logger

amo_robot = AmoCRM.AmoCRM()
bq_robot = GoogleBQ_deals.GoogleBQ_deals()
head = bq_robot.get_deals_table_head()
schema = bq_robot.get_deals_table_schema()
deals_temp_json_path = 'amo_leads_temp_json/'
deals_week_json_path = 'amo_leads_week_json/'
deals_week_json_filename = 'json_deals_week'
filename = 'temp_updated_leads_5.json'
pipelines_list = [7038, 28752] #идентификаторы воронок, из которых хотим забирать сделки


last_updated_at = bq_robot.get_last_updated_date()
amo_robot._get_updated_deals_since_timestamp_to_json_temp_folder(last_updated_at)

with open(deals_temp_json_path + filename, 'r', encoding="utf8") as json_file:
        updated_deals2 = json.load(json_file)

updated_deals = amo_robot._put_deals_from_raw_json_to_week_json(deals_temp_json_path, deals_week_json_path, deals_week_json_filename)

df = pd.DataFrame(columns=head)
rows = amo_robot._extract_amo_json_deals_to_dataframe(updated_deals, pipelines_list)
df = df.append(rows, ignore_index=True)

#удаляем дубликаты записей
result_df = df.drop_duplicates(keep='last')
#отправляем результирующий датафрейм в BigQuery
bq_robot.merge_deals_dataframe_to_bigquery(result_df)
#bq_robot._create_table_from_dataframe(result_df, 'ds_temp', 'tb_amo_deals', schema)

logger.info('Скрипт update_deals_from_amo_crm.py успешно завершён')