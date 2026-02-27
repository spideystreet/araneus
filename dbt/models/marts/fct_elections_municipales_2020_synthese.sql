WITH t1 AS (
    SELECT * FROM {{ ref('stg_dgouv__elections_2020_t1') }}
),

t2 AS (
    SELECT * FROM {{ ref('stg_dgouv__elections_2020_t2') }}
),

unifiés AS (
    -- On prend le T2 s'il existe, sinon le T1 (cas d'élection au premier tour)
    SELECT 
        city_id,
        city_name,
        dept_code,
        candidate_last_name,
        candidate_first_name,
        party_nuance,
        votes_count,
        total_expressed_votes,
        {{ calculate_vote_ratio('votes_count', 'total_expressed_votes') }} AS score_percentage,
        2 AS tour
    FROM t2
    
    UNION ALL
    
    SELECT 
        city_id,
        city_name,
        dept_code,
        candidate_last_name,
        candidate_first_name,
        party_nuance,
        votes_count,
        total_expressed_votes,
        {{ calculate_vote_ratio('votes_count', 'total_expressed_votes') }} AS score_percentage,
        1 AS tour
    FROM t1
    WHERE city_id NOT IN (SELECT city_id FROM t2)
),

classement AS (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY city_id ORDER BY votes_count DESC) AS rang
    FROM unifiés
)

SELECT 
    *,
    CASE WHEN rang = 1 THEN TRUE ELSE FALSE END AS is_winner
FROM classement
