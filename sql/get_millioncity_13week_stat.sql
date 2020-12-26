WITH

week_37_49 AS
(SELECT
"москва" AS city,
SUM(leads_msk) AS leads_total,
SUM(leads_msk_yandex) AS leads_yandex,
SUM(leads_msk_google) AS leads_google,
0 AS ya_cost,
0 AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"санкт-петербург" AS city,
SUM(leads_spb),
SUM(leads_spb_yandex),
SUM(leads_spb_google),
ROUND(SUM(spb_ya_cost), 2) AS ya_cost,
ROUND(SUM(spb_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"вологда" AS city,
SUM(leads_vlgd),
SUM(leads_vlgd_yandex),
SUM(leads_vlgd_google),
ROUND(SUM(vlgd_ya_cost), 2) AS ya_cost,
ROUND(SUM(vlgd_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"новосибирск" AS city,
SUM(leads_nvsbrsk),
SUM(leads_nvsbrsk_yandex),
SUM(leads_nvsbrsk_google),
ROUND(SUM(nvsbrsk_ya_cost), 2) AS ya_cost,
ROUND(SUM(nvsbrsk_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"екатеринбург" AS city,
SUM(leads_ekb),
SUM(leads_ekb_yandex),
SUM(leads_ekb_google),
ROUND(SUM(ekb_ya_cost), 2) AS ya_cost,
ROUND(SUM(ekb_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"нижний новгород" AS city,
SUM(leads_nnvgrd),
SUM(leads_nnvgrd_yandex),
SUM(leads_nnvgrd_google),
ROUND(SUM(nnvgrd_ya_cost), 2) AS ya_cost,
ROUND(SUM(nnvgrd_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"казань" AS city,
SUM(leads_kzn),
SUM(leads_kzn_yandex),
SUM(leads_kzn_google),
ROUND(SUM(kzn_ya_cost), 2) AS ya_cost,
ROUND(SUM(kzn_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"самара" AS city,
SUM(leads_smr),
SUM(leads_smr_yandex),
SUM(leads_smr_google),
ROUND(SUM(smr_ya_cost), 2) AS ya_cost,
ROUND(SUM(smr_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"омск" AS city,
SUM(leads_omsk) AS leads_omsk,
SUM(leads_omsk_yandex) AS leads_omsk_yandex,
SUM(leads_omsk_google) AS leads_omsk_google,
ROUND(SUM(omsk_ya_cost), 2) AS ya_cost,
ROUND(SUM(omsk_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"челябинск" AS city,
SUM(leads_chlbnsk),
SUM(leads_chlbnsk_yandex),
SUM(leads_chlbnsk_google),
ROUND(SUM(chlbnsk_ya_cost), 2) AS ya_cost,
ROUND(SUM(chlbnsk_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"ростов-на-дону" AS city,
SUM(leads_rstvndn),
SUM(leads_rstvndn_yandex),
SUM(leads_rstvndn_google),
ROUND(SUM(rstvndn_ya_cost), 2) AS ya_cost,
ROUND(SUM(rstvndn_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"уфа" AS city,
SUM(leads_ufa),
SUM(leads_ufa_yandex),
SUM(leads_ufa_google),
ROUND(SUM(ufa_ya_cost), 2) AS ya_cost,
ROUND(SUM(ufa_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"волгоград" AS city,
SUM(leads_vlggrd),
SUM(leads_vlggrd_yandex),
SUM(leads_vlggrd_google),
ROUND(SUM(vlggrd_ya_cost), 2) AS ya_cost,
ROUND(SUM(vlggrd_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"пермь" AS city,
SUM(leads_perm) AS leads_perm,
SUM(leads_perm_yandex) AS leads_perm_yandex,
SUM(leads_perm_google) AS leads_perm_google,
ROUND(SUM(perm_ya_cost), 2) AS ya_cost,
ROUND(SUM(perm_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"красноярск" AS city,
SUM(leads_krsnyrsk),
SUM(leads_krsnyrsk_yandex),
SUM(leads_krsnyrsk_google),
ROUND(SUM(krsnyrsk_ya_cost), 2) AS ya_cost,
ROUND(SUM(krsnyrsk_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49
UNION ALL

SELECT
"воронеж" AS city,
SUM(leads_vrnj),
SUM(leads_vrnj_yandex),
SUM(leads_vrnj_google),
ROUND(SUM(vrnj_ya_cost), 2) AS ya_cost,
ROUND(SUM(vrnj_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 37 AND 49

ORDER BY leads_total),

week_24_36 AS
(SELECT
"москва" AS city,
SUM(leads_msk) AS leads_total,
SUM(leads_msk_yandex) AS leads_yandex,
SUM(leads_msk_google) AS leads_google,
0 AS ya_cost,
0 AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"санкт-петербург" AS city,
SUM(leads_spb),
SUM(leads_spb_yandex),
SUM(leads_spb_google),
ROUND(SUM(spb_ya_cost), 2) AS ya_cost,
ROUND(SUM(spb_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"вологда" AS city,
SUM(leads_vlgd),
SUM(leads_vlgd_yandex),
SUM(leads_vlgd_google),
ROUND(SUM(vlgd_ya_cost), 2) AS ya_cost,
ROUND(SUM(vlgd_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"новосибирск" AS city,
SUM(leads_nvsbrsk),
SUM(leads_nvsbrsk_yandex),
SUM(leads_nvsbrsk_google),
ROUND(SUM(nvsbrsk_ya_cost), 2) AS ya_cost,
ROUND(SUM(nvsbrsk_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"екатеринбург" AS city,
SUM(leads_ekb),
SUM(leads_ekb_yandex),
SUM(leads_ekb_google),
ROUND(SUM(ekb_ya_cost), 2) AS ya_cost,
ROUND(SUM(ekb_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"нижний новгород" AS city,
SUM(leads_nnvgrd),
SUM(leads_nnvgrd_yandex),
SUM(leads_nnvgrd_google),
ROUND(SUM(nnvgrd_ya_cost), 2) AS ya_cost,
ROUND(SUM(nnvgrd_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"казань" AS city,
SUM(leads_kzn),
SUM(leads_kzn_yandex),
SUM(leads_kzn_google),
ROUND(SUM(kzn_ya_cost), 2) AS ya_cost,
ROUND(SUM(kzn_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"самара" AS city,
SUM(leads_smr),
SUM(leads_smr_yandex),
SUM(leads_smr_google),
ROUND(SUM(smr_ya_cost), 2) AS ya_cost,
ROUND(SUM(smr_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"омск" AS city,
SUM(leads_omsk) AS leads_omsk,
SUM(leads_omsk_yandex) AS leads_omsk_yandex,
SUM(leads_omsk_google) AS leads_omsk_google,
ROUND(SUM(omsk_ya_cost), 2) AS ya_cost,
ROUND(SUM(omsk_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"челябинск" AS city,
SUM(leads_chlbnsk),
SUM(leads_chlbnsk_yandex),
SUM(leads_chlbnsk_google),
ROUND(SUM(chlbnsk_ya_cost), 2) AS ya_cost,
ROUND(SUM(chlbnsk_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"ростов-на-дону" AS city,
SUM(leads_rstvndn),
SUM(leads_rstvndn_yandex),
SUM(leads_rstvndn_google),
ROUND(SUM(rstvndn_ya_cost), 2) AS ya_cost,
ROUND(SUM(rstvndn_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"уфа" AS city,
SUM(leads_ufa),
SUM(leads_ufa_yandex),
SUM(leads_ufa_google),
ROUND(SUM(ufa_ya_cost), 2) AS ya_cost,
ROUND(SUM(ufa_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"волгоград" AS city,
SUM(leads_vlggrd),
SUM(leads_vlggrd_yandex),
SUM(leads_vlggrd_google),
ROUND(SUM(vlggrd_ya_cost), 2) AS ya_cost,
ROUND(SUM(vlggrd_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"пермь" AS city,
SUM(leads_perm) AS leads_perm,
SUM(leads_perm_yandex) AS leads_perm_yandex,
SUM(leads_perm_google) AS leads_perm_google,
ROUND(SUM(perm_ya_cost), 2) AS ya_cost,
ROUND(SUM(perm_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"красноярск" AS city,
SUM(leads_krsnyrsk),
SUM(leads_krsnyrsk_yandex),
SUM(leads_krsnyrsk_google),
ROUND(SUM(krsnyrsk_ya_cost), 2) AS ya_cost,
ROUND(SUM(krsnyrsk_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36
UNION ALL

SELECT
"воронеж" AS city,
SUM(leads_vrnj),
SUM(leads_vrnj_yandex),
SUM(leads_vrnj_google),
ROUND(SUM(vrnj_ya_cost), 2) AS ya_cost,
ROUND(SUM(vrnj_goo_cost)/1000000, 2) AS goo_cost
FROM ds_SA_weekly.tb_millioncity_cntx_stat_weekly
WHERE
year_num = 2020 AND
week_num BETWEEN 24 AND 36

ORDER BY leads_total)

SELECT *
FROM
week_37_49
FULL OUTER JOIN
week_24_36
USING(city)
ORDER BY
(week_37_49.leads_total + week_24_36.leads_total)