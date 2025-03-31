{{ config(
    materialized='view'
) }}

with newdata as( 
    select *
    from `core-sprite-407216.review_google_playstore.google_review_result`
    where Cast(`at` as datetime) >= CURRENT_DATETIME()-INTERVAL 1 DAY
    and reviewId is not null
)
select * from newdata


