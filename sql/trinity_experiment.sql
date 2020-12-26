#Triniry_Experiment
#Анализируемые воронки:
DECLARE Pipelines_List ARRAY <INT64>;
#Список отслеживаемых городов:
DECLARE Cities_List_Black ARRAY <STRING>;
DECLARE Cities_List_Green ARRAY <STRING>;
DECLARE Cities_List_Blue ARRAY <STRING>;
#Список отслеживаемых кампаний:
DECLARE Yandex_List_Black ARRAY <INT64>;
DECLARE Google_List_Black ARRAY <INT64>;
DECLARE Yandex_List_Green ARRAY <INT64>;
DECLARE Google_List_Green ARRAY <INT64>;
DECLARE Yandex_List_Blue ARRAY <INT64>;
DECLARE Google_List_Blue ARRAY <INT64>;

SET Pipelines_List = [7038];

SET Cities_List_Black = ["самара", "омск", "волгоград", "красноярск", "краснодар", "сургут", "прокопьевск", "энгельск", "северодвинск"];
SET Yandex_List_Black = [16678514, 46624557, 16678613, 16678554, 40335492, 16679018, 46924091, 16679436, 16700818, 55317380, 55317387, 55317400];
SET Google_List_Black = [773449408, 1911696877, 773449816, 773989869, 773449411, 773989890, 773449621, 773449597];

SET Cities_List_Green = ["екатеринбург", "казань", "ростов-на-дону", "ростов на дону", "уфа", "пермь", "курган"];
SET Yandex_List_Green = [16678454, 16678474, 16678526, 16678534, 16678593, 16679074, 55317379, 55317386, 55317396];
SET Google_List_Green = [773449831, 773449849, 773449390, 773449624, 773449378, 773449417, 1917730947];

SET Cities_List_Blue = ["нижний новгород", "челябинск", "воронеж", "саратов", "нижневартовск", "новочеркасск", "новочеркаск"];
SET Yandex_List_Blue = [16678461, 16678484, 16678606, 16678638, 46924317, 46924359, 55317384, 55317388, 55317393];
SET Google_List_Blue = [773449585, 773449819, 773449405, 773449843];

WITH
Google_Stat_Black AS
(SELECT
EXTRACT(YEAR FROM Date) AS year_num,
EXTRACT(ISOWEEK FROM Date) AS week_num,
SUM(IF(CampaignId IN UNNEST(Google_List_Black), Impressions, 0)) AS goo_imps_black,
SUM(IF(CampaignId IN UNNEST(Google_List_Black), Clicks, 0)) AS goo_clicks_black,
SUM(IF(CampaignId IN UNNEST(Google_List_Black), Cost, 0)) AS goo_cost_black,

SUM(IF(CampaignId IN UNNEST(Google_List_Blue), Impressions, 0)) AS goo_imps_blue,
SUM(IF(CampaignId IN UNNEST(Google_List_Blue), Clicks, 0)) AS goo_clicks_blue,
SUM(IF(CampaignId IN UNNEST(Google_List_Blue), Cost, 0)) AS goo_cost_blue,

SUM(IF(CampaignId IN UNNEST(Google_List_Green), Impressions, 0)) AS goo_imps_green,
SUM(IF(CampaignId IN UNNEST(Google_List_Green), Clicks, 0)) AS goo_clicks_green,
SUM(IF(CampaignId IN UNNEST(Google_List_Green), Cost, 0)) AS goo_cost_green

FROM
ds_SA_daily.tb_GooAds_base_daily
GROUP BY
year_num, week_num
ORDER BY
year_num, week_num),

Yandex_Stat_Black AS
(SELECT
EXTRACT(YEAR FROM Date) AS year_num,
EXTRACT(ISOWEEK FROM Date) AS week_num,

SUM(IF(CampaignId IN UNNEST(Yandex_List_Black), Impressions, 0)) AS ya_imps_black,
SUM(IF(CampaignId IN UNNEST(Yandex_List_Black), Clicks, 0)) AS ya_clicks_black,
SUM(IF(CampaignId IN UNNEST(Yandex_List_Black), Cost, 0)) AS ya_cost_black,

SUM(IF(CampaignId IN UNNEST(Yandex_List_Blue), Impressions, 0)) AS ya_imps_blue,
SUM(IF(CampaignId IN UNNEST(Yandex_List_Blue), Clicks, 0)) AS ya_clicks_blue,
SUM(IF(CampaignId IN UNNEST(Yandex_List_Blue), Cost, 0)) AS ya_cost_blue,

SUM(IF(CampaignId IN UNNEST(Yandex_List_Green), Impressions, 0)) AS ya_imps_green,
SUM(IF(CampaignId IN UNNEST(Yandex_List_Green), Clicks, 0)) AS ya_clicks_green,
SUM(IF(CampaignId IN UNNEST(Yandex_List_Green), Cost, 0)) AS ya_cost_green

FROM
ds_SA_daily.tb_YaDir_base_daily

GROUP BY
year_num, week_num
ORDER BY
year_num, week_num),

Amo_Stat_Prep AS
(SELECT 
SAFE_CAST(SPLIT(lead_utm_term, "-")[SAFE_ORDINAL(1)] AS INT64) AS campaign_id,
amo_city,
created_at_timestamp,
lead_utm_source,
amo_deal_id
FROM
ds_synapse_analytics.tb_amo_deals),

Amo_Stat AS
(SELECT
EXTRACT(YEAR FROM created_at_timestamp) as year_num,
EXTRACT(ISOWEEK FROM created_at_timestamp) as week_num,
COUNT(amo_deal_id) AS leads_total,
COUNTIF(lead_utm_source = "yandex" OR lead_utm_source = "google") AS leads_adv_total,

COUNTIF(LOWER(amo_city) IN UNNEST(Cities_List_Black)) AS leads_from_geo_black,
COUNTIF(campaign_id IN UNNEST(Yandex_List_Black)) AS leads_yandex_black,
COUNTIF(campaign_id IN UNNEST(Google_List_Black)) AS leads_google_black,

COUNTIF(LOWER(amo_city) IN UNNEST(Cities_List_Green)) AS leads_from_geo_green,
COUNTIF(campaign_id IN UNNEST(Yandex_List_Green)) AS leads_yandex_green,
COUNTIF(campaign_id IN UNNEST(Google_List_Green)) AS leads_google_green,

COUNTIF(LOWER(amo_city) IN UNNEST(Cities_List_Blue)) AS leads_from_geo_blue,
COUNTIF(campaign_id IN UNNEST(Yandex_List_Blue)) AS leads_yandex_blue,
COUNTIF(campaign_id IN UNNEST(Google_List_Blue)) AS leads_google_blue
FROM
Amo_Stat_Prep
GROUP BY
year_num, week_num
ORDER BY
year_num, week_num)

SELECT
*
FROM
Yandex_Stat_Black
FULL OUTER JOIN Amo_Stat
USING(year_num, week_num)
FULL OUTER JOIN Google_Stat_Black
USING(year_num, week_num)  
ORDER BY
year_num, week_num