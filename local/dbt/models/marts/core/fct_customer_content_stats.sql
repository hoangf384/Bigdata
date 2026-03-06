{{ config(materialized='table') }}

WITH enriched_logs AS (
    SELECT 
        contract,
        log_date,
        total_duration,
        CASE 
            WHEN app_name IN ('CHANNEL', 'DSHD', 'KPLUS', 'KPlus') THEN 'Truyen Hinh'
            WHEN app_name IN ('VOD', 'FIMS_RES', 'BHD_RES', 'VOD_RES', 'FIMS', 'BHD', 'DANET') THEN 'Phim Truyen'
            WHEN app_name = 'RELAX' THEN 'Giai Tri'
            WHEN app_name = 'CHILD' THEN 'Thieu Nhi'
            WHEN app_name = 'SPORT' THEN 'The Thao'
            ELSE 'Other'
        END AS content_type
    FROM {{ ref('fct_content_logs') }}
    WHERE contract != '0'
),

customer_pivot AS (
    SELECT 
        contract,
        COUNT(DISTINCT log_date) AS active_days,
        SUM(CASE WHEN content_type = 'Truyen Hinh' THEN total_duration ELSE 0 END) AS duration_truyen_hinh,
        SUM(CASE WHEN content_type = 'Phim Truyen' THEN total_duration ELSE 0 END) AS duration_phim_truyen,
        SUM(CASE WHEN content_type = 'The Thao' THEN total_duration ELSE 0 END) AS duration_the_thao,
        SUM(CASE WHEN content_type = 'Thieu Nhi' THEN total_duration ELSE 0 END) AS duration_thieu_nhi,
        SUM(CASE WHEN content_type = 'Giai Tri' THEN total_duration ELSE 0 END) AS duration_giai_tri
    FROM enriched_logs
    WHERE content_type != 'Other'
    GROUP BY 1
),
final_stats AS (
    SELECT 
        *,
        GREATEST(
            duration_truyen_hinh, 
            duration_phim_truyen, 
            duration_the_thao, 
            duration_thieu_nhi, 
            duration_giai_tri
        ) AS max_duration
    FROM customer_pivot
)

SELECT 
    contract,
    active_days,
    duration_truyen_hinh,
    duration_phim_truyen,
    duration_the_thao,
    duration_thieu_nhi,
    duration_giai_tri,
    CASE 
        WHEN duration_truyen_hinh = max_duration AND max_duration > 0 THEN 'Truyen Hinh'
        WHEN duration_phim_truyen = max_duration AND max_duration > 0 THEN 'Phim Truyen'
        WHEN duration_the_thao = max_duration AND max_duration > 0 THEN 'The Thao'
        WHEN duration_thieu_nhi = max_duration AND max_duration > 0 THEN 'Thieu Nhi'
        WHEN duration_giai_tri = max_duration AND max_duration > 0 THEN 'Giai Tri'
        ELSE 'None'
    END AS most_watch,
    CONCAT_WS('-',
        CASE WHEN duration_giai_tri > 0 THEN 'Giai Tri' END,
        CASE WHEN duration_phim_truyen > 0 THEN 'Phim Truyen' END,
        CASE WHEN duration_the_thao > 0 THEN 'The Thao' END,
        CASE WHEN duration_thieu_nhi > 0 THEN 'Thieu Nhi' END,
        CASE WHEN duration_truyen_hinh > 0 THEN 'Truyen Hinh' END
    ) AS taste
FROM final_stats
