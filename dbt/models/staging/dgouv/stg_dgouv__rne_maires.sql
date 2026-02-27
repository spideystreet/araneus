WITH source AS (
    SELECT * FROM {{ source('dgouv', 'rne_maires') }}
),

renamed AS (
    SELECT
        "Code du département" AS dept_code,
        "Libellé du département" AS dept_name,
        "Code de la commune" AS city_code,
        "Libellé de la commune" AS city_name,
        "Nom de l'élu" AS last_name,
        "Prénom de l'élu" AS first_name,
        "Code sexe" AS gender,
        TO_DATE("Date de naissance", 'DD/MM/YYYY') AS birth_date,
        "Code de la catégorie socio-professionnelle" AS csp_code,
        "Libellé de la catégorie socio-professionnelle" AS csp_name,
        TO_DATE("Date de début du mandat", 'DD/MM/YYYY') AS mandate_start_date,
        TO_DATE("Date de début de la fonction", 'DD/MM/YYYY') AS function_start_date
    FROM source
)

SELECT * FROM renamed
