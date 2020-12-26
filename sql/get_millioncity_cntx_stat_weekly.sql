#get_millioncity_cntx_stat_weekly

#Списки кампаний в яндексе:
DECLARE Vlggrd_Ya_List ARRAY <INT64>;
DECLARE Chlbnsk_Ya_List ARRAY <INT64>;
DECLARE Perm_Ya_List ARRAY <INT64>;
DECLARE Krsnyrsk_Ya_List ARRAY <INT64>;
DECLARE Rstvndn_Ya_List ARRAY <INT64>;
DECLARE Vrnj_Ya_List ARRAY <INT64>;
DECLARE Kzn_Ya_List ARRAY <INT64>;
DECLARE Smr_Ya_List ARRAY <INT64>;
DECLARE Ufa_Ya_List ARRAY <INT64>;
DECLARE Omsk_Ya_List ARRAY <INT64>;
DECLARE Nnvgrd_Ya_List ARRAY <INT64>;
DECLARE Nvsbrsk_Ya_List ARRAY <INT64>;
DECLARE Ekb_Ya_List ARRAY <INT64>;
DECLARE Vlgd_Ya_List ARRAY <INT64>;
DECLARE Spb_Ya_List ARRAY <INT64>;

#Списки кампаний в гугле:
DECLARE Vlggrd_Goo_List ARRAY <INT64>;
DECLARE Chlbnsk_Goo_List ARRAY <INT64>;
DECLARE Perm_Goo_List ARRAY <INT64>;
DECLARE Krsnyrsk_Goo_List ARRAY <INT64>;
DECLARE Rstvndn_Goo_List ARRAY <INT64>;
DECLARE Vrnj_Goo_List ARRAY <INT64>;
DECLARE Kzn_Goo_List ARRAY <INT64>;
DECLARE Smr_Goo_List ARRAY <INT64>;
DECLARE Ufa_Goo_List ARRAY <INT64>;
DECLARE Omsk_Goo_List ARRAY <INT64>;
DECLARE Nnvgrd_Goo_List ARRAY <INT64>;
DECLARE Nvsbrsk_Goo_List ARRAY <INT64>;
DECLARE Ekb_Goo_List ARRAY <INT64>;
DECLARE Vlgd_Goo_List ARRAY <INT64>;
DECLARE Spb_Goo_List ARRAY <INT64>;

#Заполняем списки
SET Vlggrd_Ya_List = [16678613];
SET Chlbnsk_Ya_List = [16678484];
SET Perm_Ya_List = [16678593];
SET Krsnyrsk_Ya_List = [16678554, 55317387];
SET Rstvndn_Ya_List = [16678526, 55317386];
SET Vrnj_Ya_List = [16678606, 55317388];
SET Kzn_Ya_List = [16678474];
SET Smr_Ya_List = [16678514, 55317380];
SET Ufa_Ya_List = [16678534];
SET Omsk_Ya_List = [46624557];
SET Nnvgrd_Ya_List = [16678461, 55317384];
SET Nvsbrsk_Ya_List = [40335487];
SET Ekb_Ya_List = [16678454, 55317379];
SET Vlgd_Ya_List = [34434856];
SET Spb_Ya_List = [41177456];

SET Vlggrd_Goo_List = [773449816];
SET Chlbnsk_Goo_List = [773449819];
SET Perm_Goo_List = [1911696670];
SET Krsnyrsk_Goo_List = [773989869];
SET Rstvndn_Goo_List = [773449390];
SET Vrnj_Goo_List = [773449405];
SET Kzn_Goo_List = [773449849];
SET Smr_Goo_List = [773449408];
SET Ufa_Goo_List = [773449624];
SET Omsk_Goo_List = [1911696877];
SET Nnvgrd_Goo_List = [773449585];
SET Nvsbrsk_Goo_List = [1683612361];
SET Ekb_Goo_List = [773449831];
SET Vlgd_Goo_List = [1357620219];
SET Spb_Goo_List = [1892477222];

WITH

Ya_traf_stat AS
(SELECT
EXTRACT(YEAR FROM Date) AS year_num,
EXTRACT(ISOWEEK FROM Date) AS week_num,
SUM(IF(CampaignId IN UNNEST(Vlggrd_Ya_List), Impressions, 0)) AS vlggrd_ya_imps,
SUM(IF(CampaignId IN UNNEST(Vlggrd_Ya_List), Clicks, 0)) AS vlggrd_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Vlggrd_Ya_List), Cost, 0)), 2) AS vlggrd_ya_cost,

SUM(IF(CampaignId IN UNNEST(Chlbnsk_Ya_List), Impressions, 0)) AS chlbnsk_ya_imps,
SUM(IF(CampaignId IN UNNEST(Chlbnsk_Ya_List), Clicks, 0)) AS chlbnsk_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Chlbnsk_Ya_List), Cost, 0)), 2) AS chlbnsk_ya_cost,

SUM(IF(CampaignId IN UNNEST(Perm_Ya_List), Impressions, 0)) AS perm_ya_imps,
SUM(IF(CampaignId IN UNNEST(Perm_Ya_List), Clicks, 0)) AS perm_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Perm_Ya_List), Cost, 0)), 2) AS perm_ya_cost,

SUM(IF(CampaignId IN UNNEST(Krsnyrsk_Ya_List), Impressions, 0)) AS krsnyrsk_ya_imps,
SUM(IF(CampaignId IN UNNEST(Krsnyrsk_Ya_List), Clicks, 0)) AS krsnyrsk_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Krsnyrsk_Ya_List), Cost, 0)), 2) AS krsnyrsk_ya_cost,

SUM(IF(CampaignId IN UNNEST(Rstvndn_Ya_List), Impressions, 0)) AS rstvndn_ya_imps,
SUM(IF(CampaignId IN UNNEST(Rstvndn_Ya_List), Clicks, 0)) AS rstvndn_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Rstvndn_Ya_List), Cost, 0)), 2) AS rstvndn_ya_cost,

SUM(IF(CampaignId IN UNNEST(Vrnj_Ya_List), Impressions, 0)) AS vrnj_ya_imps,
SUM(IF(CampaignId IN UNNEST(Vrnj_Ya_List), Clicks, 0)) AS vrnj_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Vrnj_Ya_List), Cost, 0)), 2) AS vrnj_ya_cost,

SUM(IF(CampaignId IN UNNEST(Kzn_Ya_List), Impressions, 0)) AS kzn_ya_imps,
SUM(IF(CampaignId IN UNNEST(Kzn_Ya_List), Clicks, 0)) AS kzn_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Kzn_Ya_List), Cost, 0)), 2) AS kzn_ya_cost,

SUM(IF(CampaignId IN UNNEST(Smr_Ya_List), Impressions, 0)) AS smr_ya_imps,
SUM(IF(CampaignId IN UNNEST(Smr_Ya_List), Clicks, 0)) AS smr_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Smr_Ya_List), Cost, 0)), 2) AS smr_ya_cost,

SUM(IF(CampaignId IN UNNEST(Ufa_Ya_List), Impressions, 0)) AS ufa_ya_imps,
SUM(IF(CampaignId IN UNNEST(Ufa_Ya_List), Clicks, 0)) AS ufa_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Ufa_Ya_List), Cost, 0)), 2) AS ufa_ya_cost,

SUM(IF(CampaignId IN UNNEST(Omsk_Ya_List), Impressions, 0)) AS omsk_ya_imps,
SUM(IF(CampaignId IN UNNEST(Omsk_Ya_List), Clicks, 0)) AS omsk_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Omsk_Ya_List), Cost, 0)), 2) AS omsk_ya_cost,

SUM(IF(CampaignId IN UNNEST(Nnvgrd_Ya_List), Impressions, 0)) AS nnvgrd_ya_imps,
SUM(IF(CampaignId IN UNNEST(Nnvgrd_Ya_List), Clicks, 0)) AS nnvgrd_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Nnvgrd_Ya_List), Cost, 0)), 2) AS nnvgrd_ya_cost,

SUM(IF(CampaignId IN UNNEST(Nvsbrsk_Ya_List), Impressions, 0)) AS nvsbrsk_ya_imps,
SUM(IF(CampaignId IN UNNEST(Nvsbrsk_Ya_List), Clicks, 0)) AS nvsbrsk_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Nvsbrsk_Ya_List), Cost, 0)), 2) AS nvsbrskya_cost,

SUM(IF(CampaignId IN UNNEST(Ekb_Ya_List), Impressions, 0)) AS ekb_ya_imps,
SUM(IF(CampaignId IN UNNEST(Ekb_Ya_List), Clicks, 0)) AS ekb_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Ekb_Ya_List), Cost, 0)), 2) AS ekb_ya_cost,

SUM(IF(CampaignId IN UNNEST(Spb_Ya_List), Impressions, 0)) AS spb_ya_imps,
SUM(IF(CampaignId IN UNNEST(Spb_Ya_List), Clicks, 0)) AS spb_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Spb_Ya_List), Cost, 0)), 2) AS spb_ya_cost,

SUM(IF(CampaignId IN UNNEST(Vlgd_Ya_List), Impressions, 0)) AS vlgd_ya_imps,
SUM(IF(CampaignId IN UNNEST(Vlgd_Ya_List), Clicks, 0)) AS vlgd_ya_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Vlgd_Ya_List), Cost, 0)), 2) AS vlgd_ya_cost,

FROM ds_SA_daily.tb_YaDir_base_daily
GROUP BY year_num, week_num
ORDER BY year_num, week_num),


Goo_traf_stat AS
(SELECT
EXTRACT(YEAR FROM Date) AS year_num,
EXTRACT(ISOWEEK FROM Date) AS week_num,
SUM(IF(CampaignId IN UNNEST(Vlggrd_Goo_List), Impressions, 0)) AS vlggrd_goo_imps,
SUM(IF(CampaignId IN UNNEST(Vlggrd_Goo_List), Clicks, 0)) AS vlggrd_goo_clicks,
SUM(IF(CampaignId IN UNNEST(Vlggrd_Goo_List), Cost, 0)) AS vlggrd_goo_cost,

SUM(IF(CampaignId IN UNNEST(Chlbnsk_Goo_List), Impressions, 0)) AS chlbnsk_goo_imps,
SUM(IF(CampaignId IN UNNEST(Chlbnsk_Goo_List), Clicks, 0)) AS chlbnsk_goo_clicks,
SUM(IF(CampaignId IN UNNEST(Chlbnsk_Goo_List), Cost, 0)) AS chlbnsk_goo_cost,

SUM(IF(CampaignId IN UNNEST(Perm_Goo_List), Impressions, 0)) AS perm_goo_imps,
SUM(IF(CampaignId IN UNNEST(Perm_Goo_List), Clicks, 0)) AS perm_goo_clicks,
SUM(IF(CampaignId IN UNNEST(Perm_Goo_List), Cost, 0)) AS perm_goo_cost,

SUM(IF(CampaignId IN UNNEST(Krsnyrsk_Goo_List), Impressions, 0)) AS krsnyrsk_goo_imps,
SUM(IF(CampaignId IN UNNEST(Krsnyrsk_Goo_List), Clicks, 0)) AS krsnyrsk_goo_clicks,
SUM(IF(CampaignId IN UNNEST(Krsnyrsk_Goo_List), Cost, 0)) AS krsnyrsk_goo_cost,

SUM(IF(CampaignId IN UNNEST(Rstvndn_Goo_List), Impressions, 0)) AS rstvndn_goo_imps,
SUM(IF(CampaignId IN UNNEST(Rstvndn_Goo_List), Clicks, 0)) AS rstvndn_goo_clicks,
SUM(IF(CampaignId IN UNNEST(Rstvndn_Goo_List), Cost, 0)) AS rstvndn_goo_cost,

SUM(IF(CampaignId IN UNNEST(Vrnj_Goo_List), Impressions, 0)) AS vrnj_goo_imps,
SUM(IF(CampaignId IN UNNEST(Vrnj_Goo_List), Clicks, 0)) AS vrnj_goo_clicks,
SUM(IF(CampaignId IN UNNEST(Vrnj_Goo_List), Cost, 0)) AS vrnj_goo_cost,

SUM(IF(CampaignId IN UNNEST(Kzn_Goo_List), Impressions, 0)) AS kzn_goo_imps,
SUM(IF(CampaignId IN UNNEST(Kzn_Goo_List), Clicks, 0)) AS kzn_goo_clicks,
SUM(IF(CampaignId IN UNNEST(Kzn_Goo_List), Cost, 0)) AS kzn_goo_cost,

SUM(IF(CampaignId IN UNNEST(Smr_Goo_List), Impressions, 0)) AS smr_goo_imps,
SUM(IF(CampaignId IN UNNEST(Smr_Goo_List), Clicks, 0)) AS smr_goo_clicks,
SUM(IF(CampaignId IN UNNEST(Smr_Goo_List), Cost, 0)) AS smr_goo_cost,

SUM(IF(CampaignId IN UNNEST(Ufa_Goo_List), Impressions, 0)) AS ufa_goo_imps,
SUM(IF(CampaignId IN UNNEST(Ufa_Goo_List), Clicks, 0)) AS ufa_goo_clicks,
SUM(IF(CampaignId IN UNNEST(Ufa_Goo_List), Cost, 0)) AS ufa_goo_cost,

SUM(IF(CampaignId IN UNNEST(Omsk_Goo_List), Impressions, 0)) AS omsk_goo_imps,
SUM(IF(CampaignId IN UNNEST(Omsk_Goo_List), Clicks, 0)) AS omsk_goo_clicks,
SUM(IF(CampaignId IN UNNEST(Omsk_Goo_List), Cost, 0)) AS omsk_goo_cost,

SUM(IF(CampaignId IN UNNEST(Nnvgrd_Goo_List), Impressions, 0)) AS nnvgrd_goo_imps,
SUM(IF(CampaignId IN UNNEST(Nnvgrd_Goo_List), Clicks, 0)) AS nnvgrd_goo_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Nnvgrd_Goo_List), Cost, 0)), 2) AS nnvgrd_goo_cost,

SUM(IF(CampaignId IN UNNEST(Nvsbrsk_Goo_List), Impressions, 0)) AS nvsbrsk_goo_imps,
SUM(IF(CampaignId IN UNNEST(Nvsbrsk_Goo_List), Clicks, 0)) AS nvsbrsk_goo_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Nvsbrsk_Goo_List), Cost, 0)), 2) AS nvsbrskgoo_cost,

SUM(IF(CampaignId IN UNNEST(Ekb_Goo_List), Impressions, 0)) AS ekb_goo_imps,
SUM(IF(CampaignId IN UNNEST(Ekb_Goo_List), Clicks, 0)) AS ekb_goo_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Ekb_Goo_List), Cost, 0)), 2) AS ekb_goo_cost,

SUM(IF(CampaignId IN UNNEST(Spb_Goo_List), Impressions, 0)) AS spb_goo_imps,
SUM(IF(CampaignId IN UNNEST(Spb_Goo_List), Clicks, 0)) AS spb_goo_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Spb_Goo_List), Cost, 0)), 2) AS spb_goo_cost,

SUM(IF(CampaignId IN UNNEST(Vlgd_Goo_List), Impressions, 0)) AS vlgd_goo_imps,
SUM(IF(CampaignId IN UNNEST(Vlgd_Goo_List), Clicks, 0)) AS vlgd_goo_clicks,
ROUND(SUM(IF(CampaignId IN UNNEST(Vlgd_Goo_List), Cost, 0)), 2) AS vlgd_goo_cost,



FROM ds_SA_daily.tb_GooAds_base_daily
GROUP BY year_num, week_num
ORDER BY year_num, week_num),

Amo_Leads_Stat AS
(SELECT
EXTRACT(YEAR FROM created_at_timestamp) AS year_num,
EXTRACT(ISOWEEK FROM created_at_timestamp) AS week_num,
COUNTIF(LOWER(amo_city) LIKE "%москва%") AS leads_msk,
COUNTIF(LOWER(amo_city) LIKE "%москва%" AND lead_utm_source LIKE "%yandex%") AS leads_msk_yandex,
COUNTIF(LOWER(amo_city) LIKE "%москва%" AND lead_utm_source LIKE "%google%") AS leads_msk_google,

COUNTIF(LOWER(amo_city) LIKE "%санкт-петербург%" OR LOWER(amo_city) LIKE "%санкт петербург%" OR LOWER(amo_city) LIKE "%спб%") AS leads_spb,
COUNTIF((LOWER(amo_city) LIKE "%санкт-петербург%" OR LOWER(amo_city) LIKE "%санкт петербург%" OR LOWER(amo_city) LIKE "%спб%") AND lead_utm_source LIKE "%yandex%") AS leads_spb_yandex,
COUNTIF((LOWER(amo_city) LIKE "%санкт-петербург%" OR LOWER(amo_city) LIKE "%санкт петербург%" OR LOWER(amo_city) LIKE "%спб%") AND lead_utm_source LIKE "%google%") AS leads_spb_google,

COUNTIF(LOWER(amo_city) LIKE "%вологда%") AS leads_vlgd,
COUNTIF(LOWER(amo_city) LIKE "%вологда%" AND lead_utm_source LIKE "%yandex%") AS leads_vlgd_yandex,
COUNTIF(LOWER(amo_city) LIKE "%вологда%" AND lead_utm_source LIKE "%google%") AS leads_vlgd_google,

COUNTIF(LOWER(amo_city) LIKE "%новосибирск%") AS leads_nvsbrsk,
COUNTIF(LOWER(amo_city) LIKE "%новосибирск%" AND lead_utm_source LIKE "%yandex%") AS leads_nvsbrsk_yandex,
COUNTIF(LOWER(amo_city) LIKE "%новосибирск%" AND lead_utm_source LIKE "%google%") AS leads_nvsbrsk_google,

COUNTIF(LOWER(amo_city) LIKE "%екатеринбург%") AS leads_ekb,
COUNTIF(LOWER(amo_city) LIKE "%екатеринбург%" AND lead_utm_source LIKE "%yandex%") AS leads_ekb_yandex,
COUNTIF(LOWER(amo_city) LIKE "%екатеринбург%" AND lead_utm_source LIKE "%google%") AS leads_ekb_google,

COUNTIF(LOWER(amo_city) LIKE "%нижний новгород%") AS leads_nnvgrd,
COUNTIF(LOWER(amo_city) LIKE "%нижний новгород%" AND lead_utm_source LIKE "%yandex%") AS leads_nnvgrd_yandex,
COUNTIF(LOWER(amo_city) LIKE "%нижний новгород%" AND lead_utm_source LIKE "%google%") AS leads_nnvgrd_google,

COUNTIF(LOWER(amo_city) LIKE "%казань%") AS leads_kzn,
COUNTIF(LOWER(amo_city) LIKE "%казань%" AND lead_utm_source LIKE "%yandex%") AS leads_kzn_yandex,
COUNTIF(LOWER(amo_city) LIKE "%казань%" AND lead_utm_source LIKE "%google%") AS leads_kzn_google,

COUNTIF(LOWER(amo_city) LIKE "%самара%") AS leads_smr,
COUNTIF(LOWER(amo_city) LIKE "%самара%" AND lead_utm_source LIKE "%yandex%") AS leads_smr_yandex,
COUNTIF(LOWER(amo_city) LIKE "%самара%" AND lead_utm_source LIKE "%google%") AS leads_smr_google,

COUNTIF(LOWER(amo_city) LIKE "%омск%") AS leads_omsk,
COUNTIF(LOWER(amo_city) LIKE "%омск%" AND lead_utm_source LIKE "%yandex%") AS leads_omsk_yandex,
COUNTIF(LOWER(amo_city) LIKE "%омск%" AND lead_utm_source LIKE "%google%") AS leads_omsk_google,

COUNTIF(LOWER(amo_city) LIKE "%челябинск%") AS leads_chlbnsk,
COUNTIF(LOWER(amo_city) LIKE "%челябинск%" AND lead_utm_source LIKE "%yandex%") AS leads_chlbnsk_yandex,
COUNTIF(LOWER(amo_city) LIKE "%челябинск%" AND lead_utm_source LIKE "%google%") AS leads_chlbnsk_google,

COUNTIF(LOWER(amo_city) LIKE "%ростов-на-дону%" OR LOWER(amo_city) LIKE "%ростов на дону%") AS leads_rstvndn,
COUNTIF((LOWER(amo_city) LIKE "%ростов-на-дону%" OR LOWER(amo_city) LIKE "%ростов на дону%") AND lead_utm_source LIKE "%yandex%") AS leads_rstvndn_yandex,
COUNTIF((LOWER(amo_city) LIKE "%ростов-на-дону%" OR LOWER(amo_city) LIKE "%ростов на дону%") AND lead_utm_source LIKE "%google%") AS leads_rstvndn_google,

COUNTIF(LOWER(amo_city) LIKE "%уфа%") AS leads_ufa,
COUNTIF(LOWER(amo_city) LIKE "%уфа%" AND lead_utm_source LIKE "%yandex%") AS leads_ufa_yandex,
COUNTIF(LOWER(amo_city) LIKE "%уфа%" AND lead_utm_source LIKE "%google%") AS leads_ufa_google,


COUNTIF(LOWER(amo_city) LIKE "%волгоград%") AS leads_vlggrd,
COUNTIF(LOWER(amo_city) LIKE "%волгоград%" AND lead_utm_source LIKE "%yandex%") AS leads_vlggrd_yandex,
COUNTIF(LOWER(amo_city) LIKE "%волгоград%" AND lead_utm_source LIKE "%google%") AS leads_vlggrd_google,

COUNTIF(LOWER(amo_city) LIKE "%пермь%") AS leads_perm,
COUNTIF(LOWER(amo_city) LIKE "%пермь%" AND lead_utm_source LIKE "%yandex%") AS leads_perm_yandex,
COUNTIF(LOWER(amo_city) LIKE "%пермь%" AND lead_utm_source LIKE "%google%") AS leads_perm_google,

COUNTIF(LOWER(amo_city) LIKE "%красноярск%") AS leads_krsnyrsk,
COUNTIF(LOWER(amo_city) LIKE "%красноярск%" AND lead_utm_source LIKE "%yandex%") AS leads_krsnyrsk_yandex,
COUNTIF(LOWER(amo_city) LIKE "%красноярск%" AND lead_utm_source LIKE "%google%") AS leads_krsnyrsk_google,

COUNTIF(LOWER(amo_city) LIKE "%воронеж%") AS leads_vrnj,
COUNTIF(LOWER(amo_city) LIKE "%воронеж%" AND lead_utm_source LIKE "%yandex%") AS leads_vrnj_yandex,
COUNTIF(LOWER(amo_city) LIKE "%воронеж%" AND lead_utm_source LIKE "%google%") AS leads_vrnj_google,

FROM
ds_synapse_analytics.tb_amo_deals
WHERE
amo_pipeline_id = 7038
GROUP BY
year_num, week_num
ORDER BY
year_num, week_num)

SELECT *
FROM
Ya_traf_stat
FULL OUTER JOIN
Goo_traf_stat
USING(year_num, week_num)
FULL OUTER JOIN
Amo_Leads_Stat
USING(year_num, week_num)
ORDER BY year_num, week_num
