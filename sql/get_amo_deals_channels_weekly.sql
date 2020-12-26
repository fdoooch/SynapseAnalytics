#get_amo_deals_channels_weekly
DECLARE Inactive_Pipelines_Statuses_List ARRAY <INT64>; #статусы, в которые переходит сделка в случае, если её не удалось выиграть
SET Inactive_Pipelines_Statuses_List = [29160519, 33018708, 143, 28986483, 32363709];

WITH
AmoBaseStat AS
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

COUNTIF(amo_pipeline_id = 28752 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'search') AS web_yandex_search_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 29160522 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'search') AS web_yandex_search_trash_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 142 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'search') AS web_yandex_search_won_leads,
COUNTIF(amo_pipeline_id = 28752 AND lead_utm_source = 'google' AND lead_utm_medium = 'search') AS web_google_search_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 29160522 AND lead_utm_source = 'google' AND lead_utm_medium = 'search') AS web_google_search_trash_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 142 AND lead_utm_source = 'google' AND lead_utm_medium = 'search') AS web_google_search_won_leads,
COUNTIF(amo_pipeline_id = 28752 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'context') AS web_yandex_context_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 29160522 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'context') AS web_yandex_context_trash_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 142 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'context') AS web_yandex_context_won_leads,
COUNTIF(amo_pipeline_id = 28752 AND lead_utm_source = 'google' AND lead_utm_medium = 'context') AS web_google_context_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 29160522 AND lead_utm_source = 'google' AND lead_utm_medium = 'context') AS web_google_context_trash_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 142 AND lead_utm_source = 'google' AND lead_utm_medium = 'context') AS web_google_context_won_leads,
COUNTIF(amo_pipeline_id = 28752 AND lead_utm_source = 'yandex' AND lead_utm_medium = 're') AS web_yandex_re_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 29160522 AND lead_utm_source = 'yandex' AND lead_utm_medium = 're') AS web_yandex_re_trash_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 142 AND lead_utm_source = 'yandex' AND lead_utm_medium = 're') AS web_yandex_re_won_leads,
COUNTIF(amo_pipeline_id = 28752 AND lead_utm_source = 'google' AND lead_utm_medium = 're') AS web_google_re_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 29160522 AND lead_utm_source = 'google' AND lead_utm_medium = 're') AS web_google_re_trash_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 142 AND lead_utm_source = 'google' AND lead_utm_medium = 're') AS web_google_re_won_leads,

COUNTIF(amo_pipeline_id = 7038 AND amo_status_id IN UNNEST(Inactive_Pipelines_Statuses_List) AND (lead_utm_source = "yandex" OR lead_utm_source = "google")) AS cntx_adv_inact_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id IN UNNEST(Inactive_Pipelines_Statuses_List) AND (lead_utm_source = "yandex" OR lead_utm_source = "google")) AS web_adv_inact_leads,


COUNTIF(amo_pipeline_id = 7038 AND amo_channel LIKE '%FB%' ) AS cntx_fb_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_channel LIKE '%FB%' ) AS web_fb_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_channel LIKE '%IG%' ) AS cntx_ig_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_channel LIKE '%IG%' ) AS web_ig_leads,

FROM ds_synapse_analytics.tb_amo_deals
GROUP BY year_num, week_num)

SELECT *,
(cntx_yandex_search_leads + cntx_google_search_leads) AS cntx_search_leads,
(cntx_yandex_search_trash_leads + cntx_google_search_trash_leads) AS cntx_search_trash_leads,
(cntx_yandex_search_won_leads + cntx_google_search_won_leads) AS cntx_search_won_leads,

(cntx_yandex_context_leads + cntx_google_context_leads) AS cntx_context_leads,
(cntx_yandex_context_trash_leads + cntx_google_context_trash_leads) AS cntx_context_trash_leads,
(cntx_yandex_context_won_leads + cntx_google_context_won_leads) AS cntx_context_won_leads,

(cntx_yandex_re_leads + cntx_google_re_leads) AS cntx_re_leads,
(cntx_yandex_re_trash_leads + cntx_google_re_trash_leads) AS cntx_re_trash_leads,
(cntx_yandex_re_won_leads + cntx_google_re_won_leads) AS cntx_re_won_leads,

(web_yandex_search_leads + web_google_search_leads) AS web_search_leads,
(web_yandex_search_trash_leads + web_google_search_trash_leads) AS web_search_trash_leads,
(web_yandex_search_won_leads + web_google_search_won_leads) AS web_search_won_leads,

(web_yandex_context_leads + web_google_context_leads) AS web_context_leads,
(web_yandex_context_trash_leads + web_google_context_trash_leads) AS web_context_trash_leads,
(web_yandex_context_won_leads + web_google_context_won_leads) AS web_context_won_leads,

(web_yandex_re_leads + web_google_re_leads) AS web_re_leads,
(web_yandex_re_trash_leads + web_google_re_trash_leads) AS web_re_trash_leads,
(web_yandex_re_won_leads + web_google_re_won_leads) AS web_re_won_leads,
FROM AmoBaseStat
ORDER BY year_num, week_num