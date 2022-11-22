drop table if exists hidnani.country_ace_yy;
create table hidnani.country_ace_yy as
(
select
twp_fiscal_quarter,
twp_fiscal_week,
--conv_fiscal_qtr,
--conv_fiscal_week,
campaign_cloud,
spend_type,
geo,
ddom,
cloud_type,
offering,
market_area,
country,
channel,
tam_segment,
conversion_category,
campaign_gtm_segment,
campaign_product_segment,
campaign_category,
campaign_type,
(case when twp_fiscal_quarter=(select max(conv_fiscal_qtr) from hive.aa_target.paid_media_detailed_report) then 1 else 0 end) as qtd_filter,
sum (custom_spend_ty) as custom_spend_ty,
sum(custom_spend_ly) as custom_spend_ly,
sum(custom_spend_lq) as custom_spend_lq,
sum(custom_spend_lw) as custom_spend_lw,
sum(spend_ty) as spend_ty,
sum(spend_ly) as spend_ly,
sum(spend_lq) as spend_lq,
sum(spend_lw) as spend_lw,
sum(units_ty) as units_ty,
sum(units_ly) as units_ly,
sum(units_lq) as units_lq,
sum(units_lw) as units_lw,
sum(arr_ty) as arr_ty,
sum(arr_ly) as arr_ly,
sum(arr_lq) as arr_lq,
sum(arr_lw) as arr_lw
from
(
--TY CODE--
  (
  select
  t2.fiscal_yr_and_qtr_desc as twp_fiscal_quarter,
  t2.fiscal_yr_and_wk_desc as twp_fiscal_week,
  --conv_fiscal_qtr,
  --conv_fiscal_week,
  campaign_cloud,
  spend_type,
  geo,
  ddom,
  cloud_type,
  offering,
  market_area,
  country,
  channel,
  tam_segment,
  conversion_category,
  campaign_gtm_segment,
  campaign_product_segment,
  campaign_category,
  campaign_type,
  sum (case when spend_type = 'ROI' and upper(campaign_GTM_segment) in ('INDIVIDUAL','PHOTOGRAPHY','STUDENT','TEAM','ACROBAT DC') then spend else 0 end) as custom_spend_ty,
  sum(0) as custom_spend_ly,
  sum(0) as custom_spend_lq,
  sum(0) as custom_spend_lw,
  sum(spend) as spend_ty,
  sum(0) as spend_ly,
  sum(0) as spend_lq,
  sum(0) as spend_lw,
  sum(sourced_units) as units_ty,
  sum(0) as units_ly,
  sum(0) as units_lq,
  sum(0) as units_lw,
  sum(sourced_arr) as arr_ty,
  sum(0) as arr_ly,
  sum(0) as arr_lq,
  sum(0) as arr_lw
  from hive.aa_target.paid_media_detailed_report t1
  inner join
    apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
      on t2.fiscal_yr_and_wk_desc=t1.conv_fiscal_week
  where geo in ('ASIA')
  and upper(channel) not in ('TIKTOK')
  and t2.fiscal_wk_bucket
      between
        (
          select distinct fiscal_wk_bucket
          FROM apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
          where fiscal_yr_and_wk_desc ='2021-01'
        )
        and
        (
          select max(fiscal_wk_bucket)
          from hive.aa_target.paid_media_detailed_report t1
          inner join apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
          on t1.conv_fiscal_week =t2.fiscal_yr_and_wk_desc
        )
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17

  )
--END TY CODE--
union all
--LY CODE--
  (
  select
  t2.fiscal_yr_and_qtr_desc as twp_fiscal_quarter,
  t2.fiscal_yr_and_wk_desc as twp_fiscal_week,
  --conv_fiscal_qtr,
  --conv_fiscal_week,
  campaign_cloud,
  spend_type,
  geo,
  ddom,
  cloud_type,
  offering,
  market_area,
  country,
  channel,
  tam_segment,
  conversion_category,
  campaign_gtm_segment,
  campaign_product_segment,
  campaign_category,
  campaign_type,
  sum(0) as custom_spend_ty,
  sum (case when spend_type = 'ROI' and upper(campaign_GTM_segment) in ('INDIVIDUAL','PHOTOGRAPHY','STUDENT','TEAM','ACROBAT DC') then spend else 0 end) as custom_spend_ly,
  sum(0) as custom_spend_lq,
  sum(0) as custom_spend_lw,
  sum(0) as spend_ty,
  sum(spend) as spend_ly,
  sum(0) as spend_lq,
  sum(0) as spend_lw,
  sum(0) as units_ty,
  sum(sourced_units) as units_ly,
  sum(0) as units_lq,
  sum(0) as units_lw,
  sum(0) as arr_ty,
  sum(sourced_arr) as arr_ly,
  sum(0) as arr_lq,
  sum(0) as arr_lw
  from hive.aa_target.paid_media_detailed_report t1
  inner join
    apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
      on t2.fiscal_yr_and_wk_desc_ly =t1.conv_fiscal_week
  where geo in ('ASIA')
  and upper(channel) not in ('TIKTOK')
  and t2.fiscal_wk_bucket
      between
        (
          select distinct fiscal_wk_bucket
          FROM apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
          where fiscal_yr_and_wk_desc ='2021-01'
        )
        and
        (
          select max(fiscal_wk_bucket)
          from hive.aa_target.paid_media_detailed_report t1
          inner join apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
          on t1.conv_fiscal_week =t2.fiscal_yr_and_wk_desc
        )
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
  )
--END LY CODE--
union all
--LQ CODE--
  (
  select
  t2.fiscal_yr_and_qtr_desc as twp_fiscal_quarter,
  t2.fiscal_yr_and_wk_desc as twp_fiscal_week,
  --conv_fiscal_qtr,
  --conv_fiscal_week,
  campaign_cloud,
  spend_type,
  geo,
  ddom,
  cloud_type,
  offering,
  market_area,
  country,
  channel,
  tam_segment,
  conversion_category,
  campaign_gtm_segment,
  campaign_product_segment,
  campaign_category,
  campaign_type,
  sum(0) as custom_spend_ty,
  sum(0) as custom_spend_ly,
  sum (case when spend_type = 'ROI' and upper(campaign_GTM_segment) in ('INDIVIDUAL','PHOTOGRAPHY','STUDENT','TEAM','ACROBAT DC') then spend else 0 end) as custom_spend_lq,
  sum(0) as custom_spend_lw,
  sum(0) as spend_ty,
  sum(0) as spend_ly,
  sum(spend) as spend_lq,
  sum(0) as spend_lw,
  sum(0) as units_ty,
  sum(0) as units_ly,
  sum(sourced_units) as units_lq,
  sum(0) as units_lw,
  sum(0) as arr_ty,
  sum(0) as arr_ly,
  sum(sourced_arr) as arr_lq,
  sum(0) as arr_lw
  from hive.aa_target.paid_media_detailed_report t1
  inner join
    apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
      on t2.fiscal_yr_and_wk_desc_lq =t1.conv_fiscal_week
  where geo in ('ASIA')
  and upper(channel) not in ('TIKTOK')
  and t2.fiscal_wk_bucket
      between
        (
          select distinct fiscal_wk_bucket
          FROM apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
          where fiscal_yr_and_wk_desc ='2021-01'
        )
        and
        (
          select max(fiscal_wk_bucket)
          from hive.aa_target.paid_media_detailed_report t1
          inner join apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
          on t1.conv_fiscal_week =t2.fiscal_yr_and_wk_desc
        )
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
  )
--END LQ Code--
union all
--LW CODE--
  (
  select
  t2.fiscal_yr_and_qtr_desc as twp_fiscal_quarter,
  t2.fiscal_yr_and_wk_desc as twp_fiscal_week,
  --conv_fiscal_qtr,
  --conv_fiscal_week,
  campaign_cloud,
  spend_type,
  geo,
  ddom,
  cloud_type,
  offering,
  market_area,
  country,
  channel,
  tam_segment,
  conversion_category,
  campaign_gtm_segment,
  campaign_product_segment,
  campaign_category,
  campaign_type,
  sum(0) as custom_spend_ty,
  sum(0) as custom_spend_ly,
  sum(0) as custom_spend_lq,
  sum (case when spend_type = 'ROI' and upper(campaign_GTM_segment) in ('INDIVIDUAL','PHOTOGRAPHY','STUDENT','TEAM','ACROBAT DC') then spend else 0 end) as custom_spend_lw,
  sum(0) as spend_ty,
  sum(0) as spend_ly,
  sum(0) as spend_lq,
  sum(spend) as spend_lw,
  sum(0) as units_ty,
  sum(0) as units_ly,
  sum(0) as units_lq,
  sum(sourced_units) as units_lw,
  sum(0) as arr_ty,
  sum(0) as arr_ly,
  sum(0) as arr_lq,
  sum(sourced_arr) as arr_lw
  from hive.aa_target.paid_media_detailed_report t1
  inner join
    apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
      on t2.fiscal_yr_and_wk_desc_lw=t1.conv_fiscal_week
  where geo in ('ASIA')
  and upper(channel) not in ('TIKTOK')
  and t2.fiscal_wk_bucket
      between
        (
          select distinct fiscal_wk_bucket
          FROM apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
          where fiscal_yr_and_wk_desc ='2021-01'
        )
        and
        (
          select max(fiscal_wk_bucket)
          from hive.aa_target.paid_media_detailed_report t1
          inner join apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
          on t1.conv_fiscal_week =t2.fiscal_yr_and_wk_desc
        )
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17
  )
--END LW CODE--
) as t1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18
);
