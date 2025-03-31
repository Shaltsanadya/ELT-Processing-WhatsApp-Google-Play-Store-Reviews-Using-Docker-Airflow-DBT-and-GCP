{{ config(materialized='table') }}

WITH data as(
        select reviewId, userName, content, score,
               reviewCreatedVersion, `at`,
               repliedAt, 
        Case when score >= 0 and score <= 2 then 'Negative'
        when score = 3 then 'Neutral'
        when score >= 4 and score <= 5 then 'Positive'
        else 'Unknown' end as sentiment,
        FORMAT_DATE('%Y-%m-%d', DATE(`at`)) AS reviewdate,
        FORMAT_TIME('%H:%M:%S', TIME(`at`)) AS reviewtime
        from {{ ref ('datamart')}}
        where reviewId is not null
)

select *
from data