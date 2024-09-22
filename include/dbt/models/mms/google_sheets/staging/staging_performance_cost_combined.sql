WITH model_prev_data AS (
    -- Select all columns from the first model
    SELECT
    *
    FROM {{ ref('mms_lb_staged') }}  -- Reference the first model using ref()
),

new_source_data AS (
    -- Select all columns from the new source table
    SELECT
        Blockkodierung__TA__PG__SS__BN_,
        ET,
        Updated_Medien,
        Vermarkter___Verlag,
        Grundpreis_AR,
        Netto_AR,
        N_N_AR,
        N_N_N_AR
    FROM {{ ref('normalized_costs') }}


)

SELECT 
    a.*,
    null as impressions,
    s.ET,
    s.Vermarkter___Verlag as media_house,
    s.Grundpreis_AR as gross_media_cost,
    s.Netto_AR as net_media_cost,
    s.N_N_AR as net_net_media_cost,
    s.N_N_N_AR as media_cost_eur  
FROM model_prev_data AS a
LEFT JOIN new_source_data AS s
on
a.block_code = s.Blockkodierung__TA__PG__SS__BN_
and
a.date = s.ET
AND
LOWER(
    REGEXP_REPLACE(
      REPLACE(REPLACE(REPLACE(a.publisher, ' ', ''), '.', ''), '-', ''),
      r'[^a-zA-Z0-9]',
      ''
    )) = LOWER(
    REGEXP_REPLACE(
      REPLACE(REPLACE(REPLACE(s.Updated_Medien, ' ', ''), '.', ''), '-', ''),
      r'[^a-zA-Z0-9]',
      ''
    ))
