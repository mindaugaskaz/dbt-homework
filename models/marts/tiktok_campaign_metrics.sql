{{ config(
    materialized='incremental',
    unique_key = 'unique_id'
    ) 
}}

with tiktok_campaign_data as (
    select *
    from {{ ref('stg_tiktok_data_deduplicated') }}
),
aggregated_by_campaign_and_date as (
    select 
    stat_time_day,
    campaign_id,
    campaign_name,
    sum(impressions) as impressions,
    sum(conversions) as conversions,
    sum(clicks) as clicks,
    sum(spend) as spend
    from tiktok_campaign_data
    group by stat_time_day, campaign_id, campaign_name
),
campaigns_with_metrics as (
    select
    stat_time_day,
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    conversions,
    spend,
    case when clicks > 0 then spend/clicks end as cpc,
    case when conversions > 0 then spend/conversions end as cpa
    from aggregated_by_campaign_and_date
)
select 
    concat(stat_time_day,'-',campaign_id) as unique_id,
    stat_time_day,
    campaign_id,
    campaign_name,
    impressions,
    clicks,
    conversions,
    round(spend,2) as spend,
    round(cpc,2) as cpc,
    round(cpa,2) as cpa
from campaigns_with_metrics
{% if is_incremental() %}

    where stat_time_day >= (select max(stat_time_day) from {{ this }})

{% endif %}