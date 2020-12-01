import os
import config
import google_bigquery as bq
import json
import amo_crm as amo
from heapq import merge
import pandas as pd

def add_bq_special_fields_to_amo_deals_dataframe(df_amo_deals):
    df_amo_deals = amo.add_bq_special_fields_to_amo_deals_dataframe(df_amo_deals)
    return df_amo_deals



with open(config.BQ_TB_AMO_DEALS_RAW_SHEMA, 'r', encoding="utf8") as json_schema:
    tb_schema = json.load(json_schema)
#bq.create_table(config.BQ_DS_MAIN, config.BQ_TB_AMO_DEALS_RAW, tb_schema)

with open(config.AMO_LEADS_WEEK_JSON_PATH + 'amo_json_2020_41.json', 'r', encoding="utf8") as json_file:
    json_deals = json.load(json_file)
df = amo.get_dataframe_from_json_deals(json_deals)
df = add_bq_special_fields_to_amo_deals_dataframe(df)

print(df.loc[df['drupal_utm'] != '', ['id', 'drupal_utm', 'lead_utm_source']])
#print(f'Размер датафрейма: {df.shape}')
#bq.create_table_from_dataframe(df, config.BQ_DS_TEMP, 'table212', tb_schema)
#amo_deal_fields_list =list( merge(config.AMO_DEALS_BASE_FIELDS, set(config.AMO_DEALS_SPECIAL_FIELDS), set(config.AMO_DEALS_CUSTOM_FIELDS.keys())))

#bq.merge_bq_table_with_dataframe(df, config.BQ_DS_MAIN, config.BQ_TB_AMO_DEALS_RAW, tb_schema, amo_deal_fields_list, 'id')