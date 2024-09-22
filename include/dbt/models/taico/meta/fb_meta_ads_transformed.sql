-- Define a CTE for raw data from ads_insights
with raw_data as (
    select
        ad_id,
        campaign_id,
        date(date_start) as created_date,
        objective,
        cast(clicks as integer) as clicks,
        cast(impressions as integer) as impressions,
        cast(unique_clicks as integer) as unique_clicks,
        cast(replace(spend, ',', '.') as float64) as spend,
        cast(replace(cpm, ',', '.') as float64) as cpm,
        cast(replace(cpc, ',', '.') as float64) as cpc,
        cast(replace(conversion_values, ',', '.') as float64) as conversion_values,
        created_time,
        updated_time,
        account_name as publisher,
        account_id as property,
        optimization_goal as media_cluster,
        account_id as owner,
        campaign_name
    from {{ source('dummy_integration_staging', 'ads_insights') }}
),

-- Define a CTE for campaign data from campaigns
campaign_data as (
    select
        concat('campaign_id_', substring(id, 4)) as campaign_id, 
        name as campaign_name
    from {{ source('dummy_integration_staging', 'campaigns') }}
),

-- Main transformation logic
fb_meta_ads_transformed as (
    select
        row_number() over () as id,
        raw_data.created_date as date,
        'facebook' as channel,
        campaign_data.campaign_name as campaign,
        raw_data.publisher,
        raw_data.property,
        case
            when raw_data.objective = 'CONVERSIONS' then 'Conversion Ad'
            when raw_data.objective = 'TRAFFIC' then 'Traffic Ad'
            when raw_data.objective = 'VIDEO_VIEWS' then 'Video Ad'
            else 'Other'
        end as media_type,
        case
            when raw_data.media_cluster in ('Classical', 'CRM', 'Digital') then raw_data.media_cluster
            else 'Other'
        end as media_cluster,
        raw_data.clicks,
        raw_data.impressions,
        raw_data.unique_clicks as unique,
        raw_data.spend as media_cost_eur,
        raw_data.cpm,
        raw_data.cpc,
        0.0 as grp_circulation,
        raw_data.conversion_values as revenue,
        raw_data.owner,
        cast(raw_data.created_time as datetime) as added_at,
        cast(raw_data.updated_time as datetime) as updated_at,
        'group_placeholder' as product_group,
        'product_placeholder' as product,
        'audience_placeholder' as audience,
        raw_data.spend as gross_media_cost,
        raw_data.spend * 0.9 as net_media_cost,
        raw_data.spend * 0.8 as net_net_media_cost,
        'block_code_placeholder' as block_code
    from raw_data
    left join campaign_data on raw_data.campaign_id = campaign_data.campaign_id
)

-- Final SELECT statement
select * from fb_meta_ads_transformed


