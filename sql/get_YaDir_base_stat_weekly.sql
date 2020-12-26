#get_YaDir_base_stat_weekly
WITH
# YaDir fingarant35-direct - CNTX старые РК по городам на поиске
YaDir_CNTX_Search_fingarant AS
(SELECT
  EXTRACT(YEAR FROM Date) AS year_num,
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_cntx_search_imps,
  SUM(Clicks) AS ya_dir_cntx_search_clicks,
  IFNULL(ROUND(SUM(Cost), 2),0) AS ya_dir_cntx_search_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_keyword_device_stat_fingarant35_direct
GROUP BY
  year_num, week_num
ORDER BY
year_num, week_num),

# YaDir SinapsGoroda - CNTX новые РК по городам на поиске
YaDir_CNTX_Search_sinapsgoroda AS
(SELECT
  EXTRACT(YEAR FROM Date) AS year_num,
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_cntx_search_imps,
  SUM(Clicks) AS ya_dir_cntx_search_clicks,
  IFNULL(ROUND(SUM(Cost), 2),0) AS ya_dir_cntx_search_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_campaign_social_device_stat_sinapsgoroda
GROUP BY
  year_num, week_num
ORDER BY
  year_num, week_num),

#----------------
# YaDir Synapsekit-direct - WEB + CNTX ремаркетинг и РСЯ
YaDir_CNTX_Context_Synapsekit AS
(SELECT
  EXTRACT(YEAR FROM Date) AS year_num,
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_cntx_context_imps,
  SUM(Clicks) AS ya_dir_cntx_context_clicks,
  IFNULL(SUM(Cost),0) AS ya_dir_cntx_context_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_keyword_device_stat_synapsekit_direct
WHERE
  CampaignName LIKE '%CNTX%' and CampaignName LIKE '%Context%'
GROUP BY 
year_num, week_num
ORDER BY
year_num, week_num),

YaDir_CNTX_RE_Synapsekit AS
(SELECT
  EXTRACT(YEAR FROM Date) AS year_num,
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_cntx_re_imps,
  SUM(Clicks) AS ya_dir_cntx_re_clicks,
  IFNULL(SUM(Cost),0) AS ya_dir_cntx_re_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_keyword_device_stat_synapsekit_direct
WHERE
  CampaignName LIKE '%CNTX%' and CampaignName LIKE '%RE%'
GROUP BY 
year_num, week_num
ORDER BY
year_num, week_num),

YaDir_WEB_Context_Synapsekit AS
(SELECT
  EXTRACT(YEAR FROM Date) AS year_num,
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_context_imps,
  SUM(Clicks) AS ya_dir_web_context_clicks,
  IFNULL(SUM(Cost),0) AS ya_dir_web_context_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_keyword_device_stat_synapsekit_direct
WHERE
  CampaignName LIKE '%WEB%' and CampaignName LIKE '%Context%'
GROUP BY 
year_num, week_num
ORDER BY
year_num, week_num),

YaDir_WEB_RE_Synapsekit AS
(SELECT
  EXTRACT(YEAR FROM Date) AS year_num,
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_re_imps,
  SUM(Clicks) AS ya_dir_web_re_clicks,
  IFNULL(SUM(Cost),0) AS ya_dir_web_re_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_keyword_device_stat_synapsekit_direct
WHERE
  CampaignName LIKE '%WEB%' and CampaignName LIKE '%RE%'
GROUP BY 
year_num, week_num
ORDER BY
year_num, week_num),

#---------------------
# YaDir Synapse2015-direct - WEB старые РК
YaDir_WEB_Search_synapse2015 AS
(SELECT
  EXTRACT(YEAR FROM Date) AS year_num,
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_search_imps,
  SUM(Clicks) AS ya_dir_web_search_clicks,
  IFNULL(ROUND(SUM(Cost), 2),0) AS ya_dir_web_search_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_keyword_stat_synapse2015_direct
GROUP BY
  year_num, week_num
ORDER BY
  year_num, week_num),
  
#-----------------------------
# YaDir Synapse2020-direct - WEB новые РК поиск, контекст, мкб, ремаркетинг
YaDir_WEB_Search_synapse2020 AS
(SELECT
  EXTRACT(YEAR FROM Date) AS year_num,
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_search_imps,
  SUM(Clicks) AS ya_dir_web_search_clicks,
  IFNULL(ROUND(SUM(Cost), 2),0) AS ya_dir_web_search_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_reach_frequency_stat_synapse2020_direct
WHERE
  (CampaignName LIKE '%WEB%' or CampaignName LIKE '%ECOM.%') and CampaignName LIKE '%Search.%'
GROUP BY
  year_num, week_num
ORDER BY
  year_num, week_num),

YaDir_WEB_Context_synapse2020 AS
(SELECT
  EXTRACT(YEAR FROM Date) AS year_num,
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_context_imps,
  SUM(Clicks) AS ya_dir_web_context_clicks,
  ROUND(SUM(Cost), 2) AS ya_dir_web_context_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_reach_frequency_stat_synapse2020_direct
WHERE
  (CampaignName LIKE '%WEB%' or CampaignName LIKE '%ECOM.%') and CampaignName LIKE '%Context.%'
GROUP BY
  year_num, week_num
ORDER BY
  year_num, week_num),

YaDir_WEB_RE_synapse2020 AS
(SELECT
  EXTRACT(YEAR FROM Date) AS year_num,
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_re_imps,
  SUM(Clicks) AS ya_dir_web_re_clicks,
  IFNULL(ROUND(SUM(Cost), 2),0) AS ya_dir_web_re_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_reach_frequency_stat_synapse2020_direct
WHERE
  (CampaignName LIKE '%WEB%' or CampaignName LIKE '%ECOM.%') and CampaignName LIKE '%RE.%'
GROUP BY
  year_num, week_num
ORDER BY
  year_num, week_num),

YaDir_WEB_Media_synapse2020 AS
(SELECT
  EXTRACT(YEAR FROM Date) AS year_num,
  EXTRACT(ISOWEEK FROM Date) AS week_num,
  SUM(Impressions) AS ya_dir_web_media_imps,
  SUM(Clicks) AS ya_dir_web_media_clicks,
  IFNULL(ROUND(SUM(Cost), 2),0) AS ya_dir_web_media_cost
FROM
  ds_garpun_yandex_direct.yandex_direct_ad_reach_frequency_stat_synapse2020_direct
WHERE
  (CampaignName LIKE '%WEB%' or CampaignName LIKE '%ECOM.%') and (CampaignName LIKE '%МКБ.%' or CampaignName LIKE '%Media.%')
GROUP BY
  year_num, week_num
ORDER BY
  year_num, week_num),    
  
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
year_num, week_num,
SUM(ya_dir_cntx_search_imps) as ya_dir_cntx_search_imps,
SUM(ya_dir_cntx_search_clicks) as ya_dir_cntx_search_clicks,
IFNULL(ROUND(SUM(ya_dir_cntx_search_cost), 2),0) as ya_dir_cntx_search_cost,

SUM(ya_dir_cntx_context_imps) as ya_dir_cntx_context_imps,
SUM(ya_dir_cntx_context_clicks) as ya_dir_cntx_context_clicks,
IFNULL(ROUND(SUM(ya_dir_cntx_context_cost), 2),0) as ya_dir_cntx_context_cost,

SUM(ya_dir_cntx_re_imps) as ya_dir_cntx_re_imps,
SUM(ya_dir_cntx_re_clicks) as ya_dir_cntx_re_clicks,
IFNULL(ROUND(SUM(ya_dir_cntx_re_cost), 2),0) as ya_dir_cntx_re_cost,

FROM
YaDir_CNTX_Search
FULL OUTER JOIN
YaDir_CNTX_Context
FULL OUTER JOIN  
YaDir_CNTX_RE
USING(year_num, week_num)
USING(year_num, week_num)
GROUP BY year_num, week_num
ORDER BY year_num, week_num),

YaDir_WEB AS
(SELECT
year_num, week_num,
SUM(ya_dir_web_search_imps) as ya_dir_web_search_imps,
SUM(ya_dir_web_search_clicks) as ya_dir_web_search_clicks,
IFNULL(ROUND(SUM(ya_dir_web_search_cost), 2),0) as ya_dir_web_search_cost,

SUM(ya_dir_web_context_imps) as ya_dir_web_context_imps,
SUM(ya_dir_web_context_clicks) as ya_dir_web_context_clicks,
IFNULL(ROUND(SUM(ya_dir_web_context_cost), 2),0) as ya_dir_web_context_cost,

SUM(ya_dir_web_re_imps) as ya_dir_web_re_imps,
SUM(ya_dir_web_re_clicks) as ya_dir_web_re_clicks,
IFNULL(ROUND(SUM(ya_dir_web_re_cost), 2),0) as ya_dir_web_re_cost,

SUM(ya_dir_web_media_imps) as ya_dir_web_media_imps,
SUM(ya_dir_web_media_clicks) as ya_dir_web_media_clicks,
IFNULL(ROUND(SUM(ya_dir_web_media_cost), 2),0) as ya_dir_web_media_cost,

FROM
YaDir_WEB_Search
FULL OUTER JOIN
YaDir_WEB_Context
FULL OUTER JOIN  
YaDir_WEB_RE
FULL OUTER JOIN
YaDir_WEB_Media
USING(year_num, week_num)
USING(year_num, week_num)
USING(year_num, week_num)
GROUP BY year_num, week_num
ORDER BY year_num, week_num)

SELECT *
FROM YaDir_CNTX
FULL OUTER JOIN
YaDir_WEB
USING(year_num, week_num)