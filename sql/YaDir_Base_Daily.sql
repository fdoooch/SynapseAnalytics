#YaDir_Base_Daily

WITH

YaDir_CNTX_Search_Old AS
(SELECT
Date,
ClientLogin,
CampaignId,
CampaignName,
Impressions,
Cost,
Clicks
FROM
ds_garpun_yandex_direct.yandex_direct_ad_keyword_device_stat_fingarant35_direct),

YaDir_CNTX_Search_New AS
(SELECT
Date,
ClientLogin,
CampaignId,
CampaignName,
Impressions,
Cost,
Clicks
FROM
ds_garpun_yandex_direct.yandex_direct_campaign_social_device_stat_sinapsgoroda),

YaDir_WEB_Search_Old AS
(SELECT
Date,
ClientLogin,
CampaignId,
CampaignName,
Impressions,
Cost,
Clicks
FROM
ds_garpun_yandex_direct.yandex_direct_ad_keyword_stat_synapse2015_direct),

YaDir_WEB_Search_New AS
(SELECT
Date,
ClientLogin,
CampaignId,
CampaignName,
Impressions,
Cost,
Clicks
FROM
ds_garpun_yandex_direct.yandex_direct_ad_reach_frequency_stat_synapse2020_direct),

YaDir_Context AS
(SELECT
Date,
ClientLogin,
CampaignId,
CampaignName,
Impressions,
Cost,
Clicks
FROM
ds_garpun_yandex_direct.yandex_direct_ad_keyword_device_stat_synapsekit_direct)

SELECT * FROM YaDir_Context
UNION ALL
SELECT * FROM YaDir_CNTX_Search_New
UNION ALL
SELECT * FROM YaDir_CNTX_Search_Old
UNION ALL
SELECT * FROM YaDir_WEB_Search_New
UNION ALL
SELECT * FROM YaDir_WEB_Search_Old
ORDER BY Date