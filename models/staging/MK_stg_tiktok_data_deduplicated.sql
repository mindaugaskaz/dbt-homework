{{ config(
    materialized='incremental',
    unique_key='ad_id'

    ) 
}}

with tiktok_campaign_data as (
    select *
    from {{ ref('MK_stg_tiktok_ads_reports') }}
),
tiktok_capmaign_data_duplicates_identified as (
    select
    *,
    row_number() over (partition by ad_id, stat_time_day order by pulled_at desc) as rn
    from tiktok_campaign_data
)
select *
from tiktok_capmaign_data_duplicates_identified
where rn = 1
{% if is_incremental() %}

    and pulled_at > (select max(pulled_at) from {{ this }})

{% endif %}