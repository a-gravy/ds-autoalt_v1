select
c.person_name_id,
GROUP_CONCAT(distinct s.`SAKUHIN_PUBLIC_CODE` separator '|') as sids
from credit as c
INNER JOIN sakuhin as s
on c.`SAKUHIN_ID` = s.`SAKUHIN_ID`
group by 1
