#get_GooAds_base_daily
WITH

GooAds_CNTX_Search_Old AS
(SELECT
Date,
ExternalCustomerId,
CampaignId,
Impressions,
Cost,
Clicks,
Device
FROM
ds_GooAds_517_565_1143.CampaignBasicStats_5175651143),

GooAds_WEB_Search_Old AS
(SELECT
Date,
ExternalCustomerId,
CampaignId,
Impressions,
Cost,
Clicks,
Device
FROM
ds_GooAds_195_492_1493.CampaignBasicStats_1954921493),

GooAds_WEB_New AS
(SELECT
Date,
ExternalCustomerId,
CampaignId,
Impressions,
Cost,
Clicks,
Device
FROM
ds_GooAds_812_837_9566.CampaignBasicStats_8128379566),

GooAds_Context AS
(SELECT
Date,
ExternalCustomerId,
CampaignId,
Impressions,
Cost,
Clicks,
Device
FROM
ds_GooAds_949_268_0551.CampaignBasicStats_9492680551)

SELECT * FROM GooAds_Context
UNION ALL
SELECT * FROM GooAds_CNTX_Search_Old
UNION ALL
SELECT * FROM GooAds_WEB_New
UNION ALL
SELECT * FROM GooAds_WEB_Search_Old
ORDER BY Date