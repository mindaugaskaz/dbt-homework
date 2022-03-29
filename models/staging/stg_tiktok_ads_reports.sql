{{ config(
    materialized='incremental'
    ) 
}}


with raw_tiktok_campaigns_parsed as (
    SELECT 
    cast(_airbyte_emitted_at as datetime) as pulled_at,
    json_extract(_airbyte_data,'$.dimensions.ad_id') as ad_id,
    cast(json_extract_scalar(_airbyte_data,'$.dimensions.stat_time_day') as datetime) as stat_time_day,
    json_query(_airbyte_data,'$.metrics.adgroup_id') as adgroup_id,
    json_extract_scalar(_airbyte_data,'$.metrics.campaign_name') as campaign_name,
    cast(json_query(_airbyte_data,'$.metrics.cpc') as float64) as cpc,
    cast(json_value(_airbyte_data,'$.metrics.spend') as float64) as spend,
    json_query(_airbyte_data,'$.metrics.campaign_id') as campaign_id,
    cast(json_query(_airbyte_data,'$.metrics.conversion') as int) as conversions,
    cast(json_query(_airbyte_data,'$.metrics.impressions') as int) as impressions,
    cast(json_query(_airbyte_data,'$.metrics.clicks') as int) as clicks
    FROM `homework-data2020.raw._airbyte_raw_tiktok_ads_reports`
)
select *
from raw_tiktok_campaigns_parsed

