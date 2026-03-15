WITH enriched_search AS (
    SELECT
        t.user_id,
        t.report_month,
        t.most_search,
        COALESCE(m.category_std, 'Other') AS category
    FROM {{ ref('int_user_monthly_most_search') }} t
    LEFT JOIN {{ ref('int_standardized_mapping') }} m
        ON LOWER(TRIM(t.most_search)) = LOWER(TRIM(m.keyword))
),

month_6 AS (
    SELECT
        user_id,
        most_search AS most_search_m6,
        category AS category_m6
    FROM enriched_search
    WHERE MONTH(report_month) = 6 AND YEAR(report_month) = 2022
),

month_7 AS (
    SELECT
        user_id,
        most_search AS most_search_m7,
        category AS category_m7
    FROM enriched_search
    WHERE MONTH(report_month) = 7 AND YEAR(report_month) = 2022
)

SELECT
    COALESCE(m6.user_id, m7.user_id) AS user_id,
    m6.most_search_m6,
    m7.most_search_m7,
    m6.category_m6,
    m7.category_m7,
    CASE
        WHEN m6.most_search_m6 IS NULL THEN 'New'
        WHEN m7.most_search_m7 IS NULL THEN 'Churned'
        WHEN m6.category_m6 = m7.category_m7 THEN 'Unchanged'
        ELSE 'Changed'
    END AS taste_status
FROM month_6 m6
FULL OUTER JOIN month_7 m7 ON m6.user_id = m7.user_id
