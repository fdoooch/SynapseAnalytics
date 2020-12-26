SELECT
EXTRACT(YEAR FROM created_at_timestamp) AS year,
EXTRACT(ISOWEEK FROM created_at_timestamp) AS week_num,
COUNTIF(amo_pipeline_id = 7038) AS cntx_leads,
COUNTIF(amo_pipeline_id = 28752) AS web_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871) AS cntx_trash_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_status_id = 29160522) AS web_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'search') AS cntx_yandex_search_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'search') AS cntx_yandex_search_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND lead_utm_source = 'google' AND lead_utm_medium = 'search') AS cntx_google_search_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871 AND lead_utm_source = 'google' AND lead_utm_medium = 'search') AS cntx_google_search_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'context') AS cntx_yandex_context_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871 AND lead_utm_source = 'yandex' AND lead_utm_medium = 'context') AS cntx_yandex_context_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND lead_utm_source = 'google' AND lead_utm_medium = 'context') AS cntx_google_context_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871 AND lead_utm_source = 'google' AND lead_utm_medium = 'context') AS cntx_google_context_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND lead_utm_source = 'yandex' AND lead_utm_medium = 're') AS cntx_yandex_re_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871 AND lead_utm_source = 'yandex' AND lead_utm_medium = 're') AS cntx_yandex_re_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND lead_utm_source = 'google' AND lead_utm_medium = 're') AS cntx_google_re_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_status_id = 28985871 AND lead_utm_source = 'google' AND lead_utm_medium = 're') AS cntx_google_re_trash_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_channel LIKE '%FB%' ) AS cntx_fb_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_channel LIKE '%FB%' ) AS web_fb_leads,
COUNTIF(amo_pipeline_id = 7038 AND amo_channel LIKE '%IG%' ) AS cntx_ig_leads,
COUNTIF(amo_pipeline_id = 28752 AND amo_channel LIKE '%IG%' ) AS web_ig_leads,

FROM ds_synapse_analytics.tb_amo_deals
GROUP BY year, week_num
ORDER BY year DESC, week_num DESC