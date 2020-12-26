#get_adv_base_stat_view
  
SELECT
*
FROM 
ds_SA_weekly.tb_amo_deals_channels_weekly
FULL OUTER JOIN
ds_SA_weekly.tb_goo_ads_base_stat_weekly
FULL OUTER JOIN
ds_SA_weekly.tb_ya_dir_base_stat_weekly
USING(year_num, week_num)
USING(year_num, week_num)
ORDER BY
year_num, week_num