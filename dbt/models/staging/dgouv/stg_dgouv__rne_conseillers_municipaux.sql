WITH source AS (
    SELECT * FROM {{ source('dgouv', 'rne_conseillers_municipaux') }}
),

renamed AS (
    SELECT
        "Code du département" AS dept_code,
        "Code de la commune" AS city_code,
        "Libellé de la commune" AS city_name,
        "Nom de l'élu" AS last_name,
        "Prénom de l'élu" AS first_name,
        "Code sexe" AS gender,
        TO_DATE("Date de naissance", 'DD/MM/YYYY') AS birth_date,
        "Libellé de la fonction" AS function_label,
        TO_DATE("Date de début du mandat", 'DD/MM/YYYY') AS mandate_start_date
    FROM source
)

SELECT * FROM renamed
