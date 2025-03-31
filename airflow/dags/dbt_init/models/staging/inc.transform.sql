{{ config(
    materialized='incremental',
    unique_key='reviewId'
    sort = 'at'
) }}
with newdata as( 
    select *
    from `core-sprite-407216.review_google_playstore.google_review`
    where Cast(at as datetime) >= curent_date()
)

select * from newdata