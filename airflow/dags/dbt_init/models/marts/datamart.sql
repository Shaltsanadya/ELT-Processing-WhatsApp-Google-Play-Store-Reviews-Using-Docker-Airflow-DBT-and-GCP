{{
   config(materialized = 'table') 
}}

with datamarts as (
    select * from `core-sprite-407216.review_google_playstore.google_review_result`
)

INSERT INTO `core-sprite-407216.review_google_playstore.google_review_result`
select DISTINCT * from datamarts 
