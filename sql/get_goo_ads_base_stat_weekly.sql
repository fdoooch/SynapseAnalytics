#get_goo_ads_base_stat_weekly.sql
WITH
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
  USING(year_num, week_num)
USING(year_num, week_num)
GROUP BY year_num, week_num
),

GooAds_WEB AS
(SELECT
year_num, week_num,
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
  USING(year_num, week_num)
USING(year_num, week_num)
GROUP BY year_num, week_num
)

SELECT *,
(goo_ads_web_search_cost + goo_ads_web_context_cost + goo_ads_web_re_cost) AS goo_ads_web_cost,
(goo_ads_cntx_search_cost + goo_ads_cntx_context_cost + goo_ads_cntx_re_cost) AS goo_ads_cntx_cost,
FROM
GooAds_CNTX
FULL OUTER JOIN
GooAds_WEB
USING(year_num, week_num)
ORDER BY year_num, week_num