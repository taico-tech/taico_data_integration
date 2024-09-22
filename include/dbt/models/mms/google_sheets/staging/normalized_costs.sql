WITH source_data AS (
    SELECT
        *,
        CASE
            WHEN Medien = 'RTL2' THEN 'RTL II'
            WHEN Medien = 'Super RTL' THEN 'SUP RTL'
            WHEN Medien = 'RTL Up' THEN 'RTLup'
            WHEN Medien = 'Nitro' then 'RTL NITRO'
            when Medien = 'Tele 5' then 'TELE5'
            when Medien = 'Disney Channel' then 'DISNEY CHANNEL'
            ELSE Medien
            end as Updated_Medien
    FROM {{ source("mms", "landliebe_2024_kosten_q1") }}
    where ET is not null
)

SELECT * FROM source_data