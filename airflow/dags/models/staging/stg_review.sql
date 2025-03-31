
select score, count(*) account
from {{ ref('core_transform') }}
group by score
order by 2 desc