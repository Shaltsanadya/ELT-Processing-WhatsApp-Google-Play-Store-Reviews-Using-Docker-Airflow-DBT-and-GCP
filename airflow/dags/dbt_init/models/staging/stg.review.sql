
select score, count(*) account
from {{ ref('core.transform') }}
group by score
order by 2 desc