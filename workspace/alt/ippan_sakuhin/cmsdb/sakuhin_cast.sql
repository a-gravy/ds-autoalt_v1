select
c.`SAKUHIN_ID`,
s.`SAKUHIN_PUBLIC_CODE`,
GROUP_CONCAT(distinct person_name_id separator '|') as person_name_id_list
from credit as c
INNER JOIN sakuhin as s
on c.`SAKUHIN_ID` = s.`SAKUHIN_ID`
group by 1