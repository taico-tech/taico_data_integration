-- models/load_main_test_company.sql

-- insert into `primordial-ship-332419.dummy_integration_staging.main_test_company`
select * from {{ ref('facebook_ads_transformed') }}
