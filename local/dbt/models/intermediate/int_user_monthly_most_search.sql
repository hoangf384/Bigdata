WITH monthly_counts AS (
    SELECT
        user_id,
        keyword,
        DATE_TRUNC('month', search_datetime) AS report_month,
        COUNT(*) AS search_count
    FROM {{ ref('fct_search_logs') }}
    WHERE user_id IS NOT NULL 
      AND keyword IS NOT NULL
    GROUP BY 1, 2, 3
),

ranked_keywords AS (
    SELECT
        user_id,
        keyword,
        report_month,
        search_count,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, report_month 
            ORDER BY search_count DESC, keyword ASC
        ) as rn
    FROM monthly_counts
)

SELECT
    user_id,
    report_month,
    keyword AS most_search
FROM ranked_keywords
WHERE rn = 1
