SELECT
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
year_num, week_num