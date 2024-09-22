with source_data as (
    select
        row_number() over() as id, -- Automatic ID
        owner,
        coalesce(clicks, 0) as clicks, -- Handle null values
        source,
        coalesce(unique, 0) as unique_visitors, -- Handle null values and rename column
        channel,
        product,
        coalesce(revenue, 0) as revenue, -- Handle null values
        audience,
        campaign,
        property,
        publisher,
        coalesce(block_code, 'N/A') as block_code, -- Handle null values
        coalesce(media_type, 'N/A') as media_type, -- Handle null values
        impressions,
        media_cluster,
        product_group,
        cast(replace(cast(media_cost_eur as string), ',', '.') as decimal) as media_cost_eur, -- Convert to numeric type
        cast(replace(cast(net_media_cost as string), ',', '.') as decimal) as net_media_cost, -- Convert to numeric type
        grp_circulation,
        cast(replace(cast(gross_media_cost as string), ',', '.') as decimal) as gross_media_cost, -- Convert to numeric type
        cast(replace(cast(net_net_media_cost as string), ',', '.') as decimal) as net_net_media_cost -- Convert to numeric type
    from {{ source('mms', 'main_mms') }}
)

select * from source_data
