# config.py

# Mappings of filter names to target PostgreSQL table names
TABLE_MAPPINGS = {
    'channel': 'channels',
    'publisher': 'publishers',
    'campaign': 'campaigns',
    'media_type': 'media_types',
    'media_cluster': 'media_clusters',
    'property': 'properties',
    'audience': 'audiences',
    'product': 'products',
    'product_group': 'product_groups'
}

# Expected schema definitions for each table
TABLE_SCHEMAS = {
    'channels': {'id', 'name', 'company_id'},
    'publishers': {'id', 'name', 'company_id'},
    'campaigns': {'id', 'name', 'company_id'},
    'media_types': {'id', 'name', 'company_id'},
    'media_clusters': {'id', 'name', 'company_id'},
    'properties': {'id', 'name', 'company_id'},
    'audiences': {'id', 'name', 'company_id'},
    'products': {'id', 'name', 'company_id'},
    'product_groups': {'id', 'name', 'company_id'}
}

# Columns that must not contain null values for each table
NON_NULLABLE_COLUMNS = {
    'channels': ['name', 'company_id'],
    'publishers': ['name', 'company_id'],
    'campaigns': ['name', 'company_id'],
    'media_types': ['name', 'company_id'],
    'media_clusters': ['name', 'company_id'],
    'properties': ['name', 'company_id'],
    'audiences': ['name', 'company_id'],
    'products': ['name', 'company_id'],
    'product_groups': ['name', 'company_id']
}

# Columns that must contain unique values for each table
UNIQUE_COLUMNS = {
    'channels': ['name', 'company_id'],
    'publishers': ['name', 'company_id'],
    'campaigns': ['name', 'company_id'],
    'media_types': ['name', 'company_id'],
    'media_clusters': ['name', 'company_id'],
    'properties': ['name', 'company_id'],
    'audiences': ['name', 'company_id'],
    'products': ['name', 'company_id'],
    'product_groups': ['name', 'company_id']
}



STAGING_TABLES= {
    "staging_channels": "channel",
    "staging_publishers": "publisher",
    "staging_audiences": "audience",
    "staging_products": "product",
    "staging_product_groups": "product_group",
    "staging_campaigns": "campaign",
    "staging_media_types": "media_type",
    "staging_media_clusters": "media_cluster",
    "staging_properties": "property"
}


# config/config.py

# BigQuery project and dataset configuration
PROJECT_ID = 'primordial-ship-332419'
DATASET_ID = 'dummy_integration_staging'

# Expected fields for ads_insights table
EXPECTED_SCHEMA_ADS_INSIGHTS = {
    'ad_id', 'campaign_id', 'objective', 'clicks', 'impressions',
    'unique_clicks', 'spend', 'cpm', 'cpc', 'conversion_values',
    'created_time', 'updated_time', 'campaign_name'
}


EXPECTED_SCHEMA_TRANSFORMED_DATA = {
    'id', 'date', 'channel', 'campaign', 'publisher', 'property',
    'media_type', 'media_cluster', 'clicks', 'impressions', 'unique',
    'media_cost_eur', 'cpm', 'cpc', 'grp_circulation', 'revenue',
    'owner', 'added_at', 'updated_at', 'product_group', 'product',
    'audience', 'gross_media_cost', 'net_media_cost', 'net_net_media_cost',
    'block_code'
}

EXPECTED_SCHEMA_STAGING_TABLE = {
    'id', 'date', 'channel', 'campaign', 'publisher', 'property', 'media_type',
    'media_cluster', 'clicks', 'impressions', 'unique', 'media_cost_eur', 'cpm',
    'cpc', 'grp_circulation', 'revenue', 'owner', 'added_at', 'updated_at',
     'product', 'product_group', 'audience', 'gross_media_cost',
    'net_media_cost', 'net_net_media_cost'
}

EXPECTED_SCHEMA_PRODUCTION_TABLE = {
    'id', 'date', 'channel', 'campaign', 'publisher', 'property', 'media_type',
    'media_cluster', 'clicks', 'impressions', 'unique', 'media_cost_eur', 'cpm',
    'cpc', 'grp_circulation', 'revenue', 'owner', 'added_at', 'updated_at',
     'product', 'product_group', 'audience', 'gross_media_cost',
    'net_media_cost', 'net_net_media_cost'
}

# Expected fields for campaigns table
EXPECTED_SCHEMA_CAMPAIGNS = {
    'id', 'name', 'status', 'objective'
}

# Minimum row count required for validation
MIN_ROWS = 100
