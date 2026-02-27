WITH source AS (
    SELECT * FROM {{ source('dgouv', 'elections_2020_t1') }}
),

renamed AS (
    SELECT
        "Code du département" AS dept_code,
        "Libellé du département" AS dept_name,
        {{ clean_city_code('"Code du département"', '"Code de la commune"') }} AS city_id,
        "Libellé de la commune" AS city_name,
        CAST(NULLIF("Inscrits", '') AS INTEGER) AS total_inscribed,
        CAST(NULLIF("Abstentions", '') AS INTEGER) AS total_abstentions,
        CAST(NULLIF("Votants", '') AS INTEGER) AS total_voters,
        CAST(NULLIF("Blancs", '') AS INTEGER) AS total_white_votes,
        CAST(NULLIF("Nuls", '') AS INTEGER) AS total_null_votes,
        CAST(NULLIF("Exprimés", '') AS INTEGER) AS total_expressed_votes,
        "Code Nuance" AS party_nuance,
        "Nom" AS candidate_last_name,
        "Prénom" AS candidate_first_name,
        CAST(NULLIF("Voix", '') AS INTEGER) AS votes_count,
        CAST(REPLACE(NULLIF("% Voix/Exp", ''), ',', '.') AS NUMERIC) AS votes_percentage_exp
    FROM source
)

SELECT * FROM renamed
