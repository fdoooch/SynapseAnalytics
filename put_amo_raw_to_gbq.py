#sync_amo_raw_to_gbq
#Загружаем и обновляем базу сделок в Google Big Query
#from google.cloud import bigquery as gbq
#
import google_bigquery as gb
import amo_crm as amo
import os
import config
import pandas as pd
import json
#from google.oauth2 import service_account

def get_dataframe():
    df = pd.DataFrame(np.random.randint(0, 100, size=(8,4)), columns=list('ABCD'))
    print(df)
    return df

print('Hello, BigQuery!')

df_deals = amo.amo_get_dataframe_from_json_week('amo_json_2017_12.json')
with open(config.BQ_TB_AMO_DEALS_RAW_SHEMA, 'r', encoding="utf8") as json_schema:
    tb_schema = json.load(json_schema)
gb.update_bq_table_with_dataframe(df_deals, config.BQ_DS_MAIN, config.BQ_TB_AMO_DEALS_RAW, tb_schema, list(df_deals.columns), 'id')


# Выполняем запрос с помощью функции ((https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_gbq.html read_gbq)) в pandas, записывая результат в dataframe
#df = pd.read_gbq(query, project_id=project_id, credentials=gb.creds)

#display(df.head(5))
