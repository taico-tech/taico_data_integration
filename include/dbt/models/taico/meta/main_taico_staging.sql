-- models/taico/meta/combined_main_taico_staging.sql

{{ config(
    materialized='incremental',
    unique_key=['id', 'date', 'campaign']
) }}

-- Select data from the existing main_taico_v3 table
with existing_data as (
    select
        id,
        date,
        channel,
        campaign,
        publisher,
        property,
        media_type,
        media_cluster,
        clicks,
        impressions,
        unique,
        media_cost_eur,
        cpm,
        cpc,
        grp_circulation,
        revenue,
        owner,
        added_at,
        updated_at,
        product_group,
        product,
        audience,
        gross_media_cost,
        net_media_cost,
        net_net_media_cost
    from {{ source('taico', 'main_taico_v3') }}
),

-- Select data from the transformed source
new_data as (
    select
        id,
        date,
        channel,
        campaign,
        publisher,
        property,
        media_type,
        media_cluster,
        clicks,
        impressions,
        unique,
        media_cost_eur,
        cpm,
        cpc,
        grp_circulation,
        revenue,
        owner,
        added_at,
        updated_at,
        product_group,
        product,
        audience,
        gross_media_cost,
        net_media_cost,
        net_net_media_cost
    from {{ ref('fb_meta_ads_transformed') }}
    {% if is_incremental() %}
    where updated_at > (select max(updated_at) from {{ this }})
    {% endif %}
)

-- Combine both datasets using UNION ALL
select * from existing_data

union all

select * from new_data
