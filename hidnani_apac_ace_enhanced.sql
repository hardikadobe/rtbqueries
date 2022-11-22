drop table if exists hidnani.apac_ace_enhanced;
create table hidnani.apac_ace_enhanced as
select
  fiscal_yr_and_qtr_desc,
  fiscal_yr_and_wk_desc,
  cloud_type,
  market_area,
  country_code,
  channel,
  conversion_category,
  tp_eventtype,
  tp_event_source_type,
  offering,
  product_category,
  product_name,
  subs_offer,
  route_to_market,
  twp_status,
  sum(sarr_ty) as sarr_ty,
  sum(sarr_ly) as sarr_ly,
  sum(sarr_lq) as sarr_lq,
  sum(sarr_lw) as sarr_lw,
  sum(sourced_units_ty) as sourced_units_ty,
  sum(sourced_units_ly) as sourced_units_ly,
  sum(sourced_units_lq) as sourced_units_lq,
  sum(sourced_units_lw) as sourced_units_lw
from
(
--TY CODE--
  (
  select
  t2.fiscal_yr_and_qtr_desc,
  t2.fiscal_yr_and_wk_desc,
  cloud_type,
  market_area,
  country_code,
  channel,
  conversion_category,
  tp_eventtype,
  tp_event_source_type,
  offering,
  product_category,
  product_name,
  subs_offer,
  route_to_market,
  twp_status,
  sum(sourced_arr) as sarr_ty,
  sum(0) as sarr_ly,
  sum(0) as sarr_lq,
  sum(0) as sarr_lw,
  sum(sourced_units) as sourced_units_ty,
  sum(0) as sourced_units_ly,
  sum(0) as sourced_units_lq,
  sum(0) as sourced_units_lw
  from mio_analytics.attribution_historical_results_raw_current_for_ace_enhanced t1
  inner join
    apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
      on t2.fiscal_yr_and_wk_desc=t1.fiscal_yr_and_wk_desc
  where geo in ('ASIA')
  and upper(channel) not in ('SOCIAL','NULL')
  and upper(market_area) not in ('CHN')
  and upper(product_category) not in ('PDF SERVICES','SIGN')
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
          from mio_analytics.attribution_historical_results_raw_current_for_ace_enhanced t1
          inner join apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
          on t1.fiscal_yr_and_wk_desc =t2.fiscal_yr_and_wk_desc
        )
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

  )
--END TY CODE--
union all
--LY CODE--
  (
  select
  t2.fiscal_yr_and_qtr_desc,
  t2.fiscal_yr_and_wk_desc,
  cloud_type,
  market_area,
  country_code,
  channel,
  conversion_category,
  tp_eventtype,
  tp_event_source_type,
  offering,
  product_category,
  product_name,
  subs_offer,
  route_to_market,
  twp_status,
  sum(0) as sarr_ty,
  sum(sourced_arr) as sarr_ly,
  sum(0) as sarr_lq,
  sum(0) as sarr_lw,
  sum(0) as sourced_units_ty,
  sum(sourced_units) as sourced_units_ly,
  sum(0) as sourced_units_lq,
  sum(0) as sourced_units_lw
  from mio_analytics.attribution_historical_results_raw_current_for_ace_enhanced t1
  inner join
    apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
      on t2.fiscal_yr_and_wk_desc_ly =t1.fiscal_yr_and_wk_desc
  where geo in ('ASIA')
  and upper(channel) not in ('SOCIAL','NULL')
  and upper(market_area) not in ('CHN')
  and upper(product_category) not in ('PDF SERVICES','SIGN')
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
          from mio_analytics.attribution_historical_results_raw_current_for_ace_enhanced t1
          inner join apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
          on t1.fiscal_yr_and_wk_desc =t2.fiscal_yr_and_wk_desc
        )
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15

  )
--END LY CODE--
union all
--LQ CODE--
  (
  select
  t2.fiscal_yr_and_qtr_desc,
  t2.fiscal_yr_and_wk_desc,
  cloud_type,
  market_area,
  country_code,
  channel,
  conversion_category,
  tp_eventtype,
  tp_event_source_type,
  offering,
  product_category,
  product_name,
  subs_offer,
  route_to_market,
  twp_status,
  sum(0) as sarr_ty,
  sum(0) as sarr_ly,
  sum(sourced_arr) as sarr_lq,
  sum(0) as sarr_lw,
  sum(0) as sourced_units_ty,
  sum(0) as sourced_units_ly,
  sum(sourced_units) as sourced_units_lq,
  sum(0) as sourced_units_lw
  from mio_analytics.attribution_historical_results_raw_current_for_ace_enhanced t1
  inner join
    apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
      on t2.fiscal_yr_and_wk_desc_lq =t1.fiscal_yr_and_wk_desc
  where geo in ('ASIA')
  and upper(channel) not in ('SOCIAL','NULL')
  and upper(market_area) not in ('CHN')
  and upper(product_category) not in ('PDF SERVICES','SIGN')
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
          from mio_analytics.attribution_historical_results_raw_current_for_ace_enhanced t1
          inner join apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
          on t1.fiscal_yr_and_wk_desc=t2.fiscal_yr_and_wk_desc
        )
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
  )
--END LQ Code--
union all
--LW CODE--
  (
  select
  t2.fiscal_yr_and_qtr_desc,
  t2.fiscal_yr_and_wk_desc,
  cloud_type,
  market_area,
  country_code,
  channel,
  conversion_category,
  tp_eventtype,
  tp_event_source_type,
  offering,
  product_category,
  product_name,
  subs_offer,
  route_to_market,
  twp_status,
  sum(0) as sarr_ty,
  sum(0) as sarr_ly,
  sum(0) as sarr_lq,
  sum(sourced_arr) as sarr_lw,
  sum(0) as sourced_units_ty,
  sum(0) as sourced_units_ly,
  sum(0) as sourced_units_lq,
  sum(sourced_units) as sourced_units_lw
  from mio_analytics.attribution_historical_results_raw_current_for_ace_enhanced t1
  inner join
    apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
      on t2.fiscal_yr_and_wk_desc_lw=t1.fiscal_yr_and_wk_desc
  where geo in ('ASIA')
  and upper(channel) not in ('SOCIAL','NULL')
  and upper(market_area) not in ('CHN')
  and upper(product_category) not in ('PDF SERVICES','SIGN')
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
          from mio_analytics.attribution_historical_results_raw_current_for_ace_enhanced t1
          inner join apacsop_eandr.fiscal_yr_and_wk_ly_lq_lw t2
          on t1.fiscal_yr_and_wk_desc=t2.fiscal_yr_and_wk_desc
        )
  group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
  )
--END LW CODE--
) as t1
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
