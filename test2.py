import json
import datetime as dt
import AmoCRM

def function1(json_pack):
    print(f'(json_pack содержит {len(json_pack)} элементов')
    new_deal = json_pack[0]
    year = dt.datetime.fromtimestamp(new_deal['created_at']).isocalendar()[0]
    print(f'(json_pack содержит {len(json_pack)} элементов')


week_parser = AmoCRM.AmoCRM()
temp_json_path = week_parser.CONFIG['DEALS_TEMP_JSON_PATH']
week_json_path = week_parser.CONFIG['DEALS_WEEK_JSON_PATH']
week_json_filename =  week_parser.CONFIG['DEALS_WEEK_JSON_FILENAME']
filename = 'temp_updated_leads_5.json'
output_filename = 'output_json.json'
with open(temp_json_path + filename, 'r', encoding="utf8") as json_file:
    deals = json.load(json_file)


#    print(f'В TEMP JSON {len(deals)} сделок')
    #раскладываем сделки из временной директории по файлам JSON WEEK
    #упорядочеваем сделки в JSON_PACK - это сокращает количество итераций при проверке наличия сделки в базе
#    deals.sort(key=operator.itemgetter('created_at'))

    updated_deals =  week_parser._merge_json_pack_to_json_week_deals(deals, week_json_path, week_json_filename)