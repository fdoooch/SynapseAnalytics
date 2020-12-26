WITH 
weekNums AS
(SELECT * FROM UNNEST(GENERATE_ARRAY(1, EXTRACT(ISOWEEK FROM CURRENT_DATE()))) as week_num),


# GooAds_949-268-0551 - Context + RE для CNTX и WEB
GooAds_CNTX_RE_9492680551 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  EXTRACT(YEAR FROM Date) AS year_num,
  SUM(Impressions) AS goo_ads_cntx_re_imps,
  SUM(Clicks) AS goo_ads_cntx_re_clicks,
  IFNULL(SUM(Cost), 0) AS goo_ads_cntx_re_cost
FROM
  ds_GooAds_949_268_0551.CampaignBasicStats_9492680551
WHERE CampaignID IN (
  SELECT
    CampaignID
  FROM
    ds_GooAds_949_268_0551.Campaign_9492680551
  WHERE
    CampaignName LIKE '%CNTX%' and CampaignName LIKE '%RE%'
  GROUP BY
    CampaignID)
GROUP BY 
year_num, week_num),

GooAds_WEB_RE_9492680551 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  EXTRACT(YEAR FROM Date) AS year_num,
  SUM(Impressions) AS goo_ads_web_re_imps,
  SUM(Clicks) AS goo_ads_web_re_clicks,
  IFNULL(SUM(Cost),0) AS goo_ads_web_re_cost
FROM
  ds_GooAds_949_268_0551.CampaignBasicStats_9492680551
WHERE CampaignID IN (
  SELECT
    CampaignID
  FROM
    ds_GooAds_949_268_0551.Campaign_9492680551
  WHERE
    ((CampaignName LIKE '%WEB%') or (CampaignName LIKE '%ECOM%')) and CampaignName LIKE '%RE%'
  GROUP BY
    CampaignID)
GROUP BY 
year_num, week_num),

GooAds_CNTX_Context_9492680551 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  EXTRACT(YEAR FROM Date) AS year_num,
  SUM(Impressions) AS goo_ads_cntx_context_imps,
  SUM(Clicks) AS goo_ads_cntx_context_clicks,
  IFNULL(SUM(Cost),0) AS goo_ads_cntx_context_cost
FROM
  ds_GooAds_949_268_0551.CampaignBasicStats_9492680551
WHERE CampaignID IN (
  SELECT
    CampaignID
  FROM
    ds_GooAds_949_268_0551.Campaign_9492680551
  WHERE
    CampaignName LIKE '%CNTX%' and CampaignName LIKE '%Context%'
  GROUP BY
    CampaignID)
GROUP BY 
year_num, week_num),

GooAds_WEB_Context_9492680551 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  EXTRACT(YEAR FROM Date) AS year_num,
  SUM(Impressions) AS goo_ads_web_context_imps,
  SUM(Clicks) AS goo_ads_web_context_clicks,
   IFNULL(SUM(Cost),0) AS goo_ads_web_context_cost
FROM
  ds_GooAds_949_268_0551.CampaignBasicStats_9492680551
WHERE CampaignID IN (
  SELECT
    CampaignID
  FROM
    ds_GooAds_949_268_0551.Campaign_9492680551
  WHERE
    ((CampaignName LIKE '%WEB%') or (CampaignName LIKE '%ECOM%')) and CampaignName LIKE '%Context%'
  GROUP BY
    CampaignID)
GROUP BY 
year_num, week_num),

#----------------------------
#GooAds 517-565-1143 - Search для CNTX, старые кампании
GooAds_CNTX_Search_5175651143 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  EXTRACT(YEAR FROM Date) AS year_num,
  SUM(Impressions) AS goo_ads_cntx_search_imps,
  SUM(Clicks) AS goo_ads_cntx_search_clicks,
  IFNULL(SUM(Cost),0) AS goo_ads_cntx_search_cost
FROM
  ds_GooAds_517_565_1143.CampaignBasicStats_5175651143
GROUP BY 
year_num, week_num),

#GooAds 812-837-9566 - Search для WEB, новые кампании
GooAds_WEB_Search_8128379566 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  EXTRACT(YEAR FROM Date) AS year_num,
  SUM(Impressions) AS goo_ads_web_search_imps,
  SUM(Clicks) AS goo_ads_web_search_clicks,
  IFNULL(SUM(Cost),0) AS goo_ads_web_search_cost
FROM
  ds_GooAds_812_837_9566.CampaignBasicStats_8128379566
WHERE CampaignID IN (
  SELECT
    CampaignID
  FROM
    ds_GooAds_812_837_9566.Campaign_8128379566
  WHERE
    ((CampaignName LIKE '%WEB%') or (CampaignName LIKE '%ECOM.%')) and CampaignName LIKE '%Search.%'
  GROUP BY
    CampaignID)
GROUP BY 
year_num, week_num),

#GooAds 812-837-9566 - Context для WEB, новые кампании
GooAds_WEB_Context_8128379566 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  EXTRACT(YEAR FROM Date) AS year_num,
  SUM(Impressions) AS goo_ads_web_context_imps,
  SUM(Clicks) AS goo_ads_web_context_clicks,
  IFNULL(SUM(Cost),0) AS goo_ads_web_context_cost
FROM
  ds_GooAds_812_837_9566.CampaignBasicStats_8128379566
WHERE CampaignID IN (
  SELECT
    CampaignID
  FROM
    ds_GooAds_812_837_9566.Campaign_8128379566
  WHERE
    ((CampaignName LIKE '%WEB.%') or (CampaignName LIKE '%ECOM.%')) and CampaignName LIKE '%Context.%'
  GROUP BY
    CampaignID)
GROUP BY 
year_num, week_num),

#GooAds 812-837-9566 - RE для WEB, новые кампании
GooAds_WEB_RE_8128379566 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  EXTRACT(YEAR FROM Date) AS year_num,
  SUM(Impressions) AS goo_ads_web_re_imps,
  SUM(Clicks) AS goo_ads_web_re_clicks,
  IFNULL(SUM(Cost),0) AS goo_ads_web_re_cost
FROM
  ds_GooAds_812_837_9566.CampaignBasicStats_8128379566
WHERE CampaignID IN (
  SELECT
    CampaignID
  FROM
    ds_GooAds_812_837_9566.Campaign_8128379566
  WHERE
    ((CampaignName LIKE '%WEB.%') or (CampaignName LIKE '%ECOM.%')) and CampaignName LIKE '%RE.%'
  GROUP BY
    CampaignID)
GROUP BY 
year_num, week_num),

#GooAds 195-492-1493 - Search для WEB, старые кампании
GooAds_WEB_Search_1954921493 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  EXTRACT(YEAR FROM Date) AS year_num,
  SUM(Impressions) AS goo_ads_web_search_imps,
  SUM(Clicks) AS goo_ads_web_search_clicks,
  IFNULL(SUM(Cost),0) AS goo_ads_web_search_cost
FROM
  ds_GooAds_195_492_1493.CampaignBasicStats_1954921493
GROUP BY 
year_num, week_num),

##================
GooAds_WEB_Search AS
(SELECT * FROM GooAds_WEB_Search_1954921493 UNION ALL
SELECT * FROM GooAds_WEB_Search_8128379566),

GooAds_WEB_Context AS
(SELECT * FROM GooAds_WEB_Context_9492680551 UNION ALL
SELECT * FROM GooAds_WEB_Context_8128379566),

GooAds_WEB_RE AS
(SELECT * FROM GooAds_WEB_RE_9492680551 UNION ALL
SELECT * FROM GooAds_WEB_RE_8128379566),

##=================
GooAds_CNTX AS
(SELECT
year_num, week_num,
SUM(t_s_1.goo_ads_cntx_search_imps) as goo_ads_cntx_search_imps,
SUM(t_s_1.goo_ads_cntx_search_clicks) as goo_ads_cntx_search_clicks,
IFNULL(SUM(t_s_1.goo_ads_cntx_search_cost),0) as goo_ads_cntx_search_cost,

SUM(t_c_1.goo_ads_cntx_context_imps) as goo_ads_cntx_context_imps,
SUM(t_c_1.goo_ads_cntx_context_clicks) as goo_ads_cntx_context_clicks,
IFNULL(SUM(t_c_1.goo_ads_cntx_context_cost),0) as goo_ads_cntx_context_cost,

SUM(t_r_1.goo_ads_cntx_re_imps) as goo_ads_cntx_re_imps,
SUM(t_r_1.goo_ads_cntx_re_clicks) as goo_ads_cntx_re_clicks,
IFNULL(SUM(t_r_1.goo_ads_cntx_re_cost),0) as goo_ads_cntx_re_cost,

FROM
GooAds_CNTX_Search_5175651143 as t_s_1
FULL OUTER JOIN
GooAds_CNTX_Context_9492680551 as t_c_1
FULL OUTER JOIN  
  GooAds_CNTX_RE_9492680551 as t_r_1
  FULL OUTER JOIN
  ds_synapse_analytics.tb_goo_ads_cntx as t
  USING(year_num, week_num)
USING(year_num, week_num)
USING(year_num, week_num)
GROUP BY year_num, week_num
),

GooAds_WEB AS
(SELECT
week_num,
SUM(t_s_1.goo_ads_web_search_imps) as goo_ads_web_search_imps,
SUM(t_s_1.goo_ads_web_search_clicks) as goo_ads_web_search_clicks,
IFNULL(SUM(t_s_1.goo_ads_web_search_cost),0) as goo_ads_web_search_cost,

SUM(t_c_1.goo_ads_web_context_imps) as goo_ads_web_context_imps,
SUM(t_c_1.goo_ads_web_context_clicks) as goo_ads_web_context_clicks,
IFNULL(SUM(t_c_1.goo_ads_web_context_cost),0) as goo_ads_web_context_cost,

SUM(t_r_1.goo_ads_web_re_imps) as goo_ads_web_re_imps,
SUM(t_r_1.goo_ads_web_re_clicks) as goo_ads_web_re_clicks,
IFNULL(SUM(t_r_1.goo_ads_web_re_cost),0) as goo_ads_web_re_cost,

FROM
GooAds_WEB_Search AS t_s_1
FULL OUTER JOIN
GooAds_WEB_Context as t_c_1
FULL OUTER JOIN  
  GooAds_WEB_RE as t_r_1
  FULL OUTER JOIN
  ds_synapse_analytics.tb_goo_ads_cntx as t
  USING(week_num)
USING(week_num)
USING(week_num)
GROUP BY week_num
),

##==================
# YaDir fingarant35-direct - CNTX старые РК по городам на поиске
YaDir_CNTX_Search_fingarant AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_cntx_search_imps,
  SUM(Clicks) AS ya_dir_cntx_search_clicks,
  IFNULL(ROUND(SUM(Cost), 2),0) AS ya_dir_cntx_search_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_keyword_device_stat_fingarant35_direct
GROUP BY
  week_num),


# YaDir SinapsGoroda - CNTX новые РК по городам на поиске
YaDir_CNTX_Search_sinapsgoroda AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_cntx_search_imps,
  SUM(Clicks) AS ya_dir_cntx_search_clicks,
  IFNULL(ROUND(SUM(Cost), 2),0) AS ya_dir_cntx_search_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_campaign_social_device_stat_sinapsgoroda
GROUP BY
  week_num),

#----------------
# YaDir Synapsekit-direct - WEB + CNTX ремаркетинг и РСЯ
YaDir_CNTX_Context_Synapsekit AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_cntx_context_imps,
  SUM(Clicks) AS ya_dir_cntx_context_clicks,
  IFNULL(SUM(Cost),0) AS ya_dir_cntx_context_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_keyword_device_stat_synapsekit_direct
WHERE
  CampaignName LIKE '%CNTX%' and CampaignName LIKE '%Context%'
GROUP BY 
week_num),

YaDir_CNTX_RE_Synapsekit AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_cntx_re_imps,
  SUM(Clicks) AS ya_dir_cntx_re_clicks,
  IFNULL(SUM(Cost),0) AS ya_dir_cntx_re_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_keyword_device_stat_synapsekit_direct
WHERE
  CampaignName LIKE '%CNTX%' and CampaignName LIKE '%RE%'
GROUP BY 
week_num),

YaDir_WEB_Context_Synapsekit AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_context_imps,
  SUM(Clicks) AS ya_dir_web_context_clicks,
  IFNULL(SUM(Cost),0) AS ya_dir_web_context_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_keyword_device_stat_synapsekit_direct
WHERE
  CampaignName LIKE '%WEB%' and CampaignName LIKE '%Context%'
GROUP BY 
week_num),

YaDir_WEB_RE_Synapsekit AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_re_imps,
  SUM(Clicks) AS ya_dir_web_re_clicks,
  IFNULL(SUM(Cost),0) AS ya_dir_web_re_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_keyword_device_stat_synapsekit_direct
WHERE
  CampaignName LIKE '%WEB%' and CampaignName LIKE '%RE%'
GROUP BY 
week_num),

#---------------------

# YaDir Synapse2015-direct - WEB старые РК
YaDir_WEB_Search_synapse2015 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_search_imps,
  SUM(Clicks) AS ya_dir_web_search_clicks,
  IFNULL(ROUND(SUM(Cost), 2),0) AS ya_dir_web_search_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_keyword_stat_synapse2015_direct
GROUP BY
  week_num),
  
#-----------------------------
# YaDir Synapse2020-direct - WEB новые РК поиск, контекст, мкб, ремаркетинг
YaDir_WEB_Search_synapse2020 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_search_imps,
  SUM(Clicks) AS ya_dir_web_search_clicks,
  IFNULL(ROUND(SUM(Cost), 2),0) AS ya_dir_web_search_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_reach_frequency_stat_synapse2020_direct
WHERE
  (CampaignName LIKE '%WEB%' or CampaignName LIKE '%ECOM.%') and CampaignName LIKE '%Search.%'
GROUP BY
  week_num),

YaDir_WEB_Context_synapse2020 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_context_imps,
  SUM(Clicks) AS ya_dir_web_context_clicks,
  ROUND(SUM(Cost), 2) AS ya_dir_web_context_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_reach_frequency_stat_synapse2020_direct
WHERE
  (CampaignName LIKE '%WEB%' or CampaignName LIKE '%ECOM.%') and CampaignName LIKE '%Context.%'
GROUP BY
  week_num),

YaDir_WEB_RE_synapse2020 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_re_imps,
  SUM(Clicks) AS ya_dir_web_re_clicks,
  IFNULL(ROUND(SUM(Cost), 2),0) AS ya_dir_web_re_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_reach_frequency_stat_synapse2020_direct
WHERE
  (CampaignName LIKE '%WEB%' or CampaignName LIKE '%ECOM.%') and CampaignName LIKE '%RE.%'
GROUP BY
  week_num),

YaDir_WEB_Media_synapse2020 AS
(SELECT
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_media_imps,
  SUM(Clicks) AS ya_dir_web_media_clicks,
  IFNULL(ROUND(SUM(Cost), 2),0) AS ya_dir_web_media_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_reach_frequency_stat_synapse2020_direct
WHERE
  (CampaignName LIKE '%WEB%' or CampaignName LIKE '%ECOM.%') and (CampaignName LIKE '%МКБ.%' or CampaignName LIKE '%Media.%')
GROUP BY
  week_num),    
  
##================
YaDir_CNTX_Search AS
(SELECT * FROM YaDir_CNTX_Search_fingarant UNION ALL
SELECT * FROM YaDir_CNTX_Search_sinapsgoroda),

YaDir_CNTX_Context AS
(SELECT * FROM YaDir_CNTX_Context_Synapsekit),

YaDir_CNTX_RE AS
(SELECT * FROM YaDir_CNTX_RE_Synapsekit),

YaDir_WEB_Search AS
(SELECT * FROM YaDir_WEB_Search_synapse2015 UNION ALL
SELECT * FROM YaDir_WEB_Search_synapse2020),

YaDir_WEB_Context AS
(SELECT * FROM YaDir_WEB_Context_Synapsekit UNION ALL
SELECT * FROM YaDir_WEB_Context_synapse2020),

YaDir_WEB_RE AS
(SELECT * FROM YaDir_WEB_RE_Synapsekit UNION ALL
SELECT * FROM YaDir_WEB_RE_synapse2020),

YaDir_WEB_Media AS
(SELECT * FROM YaDir_WEB_Media_synapse2020),

##==========================
YaDir_CNTX AS
(SELECT
week_num,
SUM(ya_dir_cntx_search_imps) as ya_dir_cntx_search_imps,
SUM(ya_dir_cntx_search_clicks) as ya_dir_cntx_search_clicks,
IFNULL(SUM(ya_dir_cntx_search_cost),0) as ya_dir_cntx_search_cost,

SUM(ya_dir_cntx_context_imps) as ya_dir_cntx_context_imps,
SUM(ya_dir_cntx_context_clicks) as ya_dir_cntx_context_clicks,
IFNULL(SUM(ya_dir_cntx_context_cost),0) as ya_dir_cntx_context_cost,

SUM(ya_dir_cntx_re_imps) as ya_dir_cntx_re_imps,
SUM(ya_dir_cntx_re_clicks) as ya_dir_cntx_re_clicks,
IFNULL(SUM(ya_dir_cntx_re_cost),0) as ya_dir_cntx_re_cost,

FROM
YaDir_CNTX_Search
FULL OUTER JOIN
YaDir_CNTX_Context
FULL OUTER JOIN  
YaDir_CNTX_RE
USING(week_num)
USING(week_num)
GROUP BY week_num
),

YaDir_WEB AS
(SELECT
week_num,
SUM(ya_dir_web_search_imps) as ya_dir_web_search_imps,
SUM(ya_dir_web_search_clicks) as ya_dir_web_search_clicks,
IFNULL(SUM(ya_dir_web_search_cost),0) as ya_dir_web_search_cost,

SUM(ya_dir_web_context_imps) as ya_dir_web_context_imps,
SUM(ya_dir_web_context_clicks) as ya_dir_web_context_clicks,
IFNULL(SUM(ya_dir_web_context_cost),0) as ya_dir_web_context_cost,

SUM(ya_dir_web_re_imps) as ya_dir_web_re_imps,
SUM(ya_dir_web_re_clicks) as ya_dir_web_re_clicks,
IFNULL(SUM(ya_dir_web_re_cost),0) as ya_dir_web_re_cost,

SUM(ya_dir_web_media_imps) as ya_dir_web_media_imps,
SUM(ya_dir_web_media_clicks) as ya_dir_web_media_clicks,
IFNULL(SUM(ya_dir_web_media_cost),0) as ya_dir_web_media_cost,

FROM
YaDir_WEB_Search
FULL OUTER JOIN
YaDir_WEB_Context
FULL OUTER JOIN  
YaDir_WEB_RE
FULL OUTER JOIN
YaDir_WEB_Media
USING(week_num)
USING(week_num)
USING(week_num)
GROUP BY week_num
),
  
AmoLeads AS
(SELECT
EXTRACT(YEAR FROM created_at_timestamp) AS year_num,
EXTRACT(ISOWEEK FROM created_at_timestamp) AS week_num,
COUNTIF(amo_pipeline_id = 7038) AS cntx_leads,
COUNTIF(amo_pipeline_id = 28752) AS web_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871) AS cntx_trash_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 29160522) AS web_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 142) AS cntx_won_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 142) AS web_won_leads,
COUNTIF(amo_pipeline_id = 7038 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'search') AS cntx_yandex_search_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'search') AS cntx_yandex_search_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 142 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'search') AS cntx_yandex_search_won_leads,
COUNTIF(amo_pipeline_id = 7038 AND lead_utm_source = 'google' AND lead_utm_medium = 'search') AS cntx_google_search_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871 AND lead_utm_source = 'google' AND lead_utm_medium = 'search') AS cntx_google_search_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 142 AND lead_utm_source = 'google' AND lead_utm_medium = 'search') AS cntx_google_search_won_leads,
COUNTIF(amo_pipeline_id = 7038 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'context') AS cntx_yandex_context_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'context') AS cntx_yandex_context_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 142 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'context') AS cntx_yandex_context_won_leads,
COUNTIF(amo_pipeline_id = 7038 AND lead_utm_source = 'google' AND lead_utm_medium = 'context') AS cntx_google_context_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871 AND lead_utm_source = 'google' AND lead_utm_medium = 'context') AS cntx_google_context_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 142 AND lead_utm_source = 'google' AND lead_utm_medium = 'context') AS cntx_google_context_won_leads,
COUNTIF(amo_pipeline_id = 7038 AND lead_utm_source = 'yandex' AND lead_utm_medium = 're') AS cntx_yandex_re_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871 AND lead_utm_source = 'yandex' AND lead_utm_medium = 're') AS cntx_yandex_re_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 142 AND lead_utm_source = 'yandex' AND lead_utm_medium = 're') AS cntx_yandex_re_won_leads,
COUNTIF(amo_pipeline_id = 7038 AND lead_utm_source = 'google' AND lead_utm_medium = 're') AS cntx_google_re_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871 AND lead_utm_source = 'google' AND lead_utm_medium = 're') AS cntx_google_re_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 142 AND lead_utm_source = 'google' AND lead_utm_medium = 're') AS cntx_google_re_won_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_channel LIKE '%FB%' ) AS cntx_fb_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_channel LIKE '%FB%' ) AS web_fb_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_channel LIKE '%IG%' ) AS cntx_ig_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_channel LIKE '%IG%' ) AS web_ig_leads,

FROM ds_synapse_analytics.tb_amo_deals
GROUP BY year_num, week_num)
  
SELECT
*,
(ya_dir_cntx_search_cost + ya_dir_cntx_context_cost + ya_dir_cntx_re_cost) AS ya_dir_cntx_cost,
(ya_dir_web_search_cost + ya_dir_web_context_cost + ya_dir_web_re_cost + ya_dir_web_media_cost) AS ya_dir_web_cost,
(goo_ads_cntx_search_cost + goo_ads_cntx_context_cost + goo_ads_cntx_re_cost) AS goo_ads_cntx_cost,
(goo_ads_web_search_cost + goo_ads_web_context_cost + goo_ads_web_re_cost) AS goo_ads_web_cost,
FROM 
AmoLeads
FULL OUTER JOIN
YaDir_CNTX
FULL OUTER JOIN
YaDir_WEB
FULL OUTER JOIN
GooAds_CNTX
FULL OUTER JOIN
GooAds_WEB
	RIGHT OUTER JOIN
	weekNums
	USING(week_num)
USING(week_num)
USING(week_num)
USING(week_num)
USING(week_num)
ORDER BY
week_num