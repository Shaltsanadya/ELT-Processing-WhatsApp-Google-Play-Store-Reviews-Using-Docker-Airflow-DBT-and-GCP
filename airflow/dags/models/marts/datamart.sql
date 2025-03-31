{{ config(materialized='incremental', unique_key='reviewId') }}

SELECT DISTINCT * 
FROM `core-sprite-407216.review_google_playstore.google_review_result`
WHERE TRUE
{% if is_incremental() %}
    AND `at` > (SELECT MAX(`at`) FROM {{ this }})
{% endif %}
