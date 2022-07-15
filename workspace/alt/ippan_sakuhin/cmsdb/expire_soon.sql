select
sakuhin_public_code,
display_name,
min(PUBLIC_START_DATETIME) as PUBLIC_START_DATETIME,
max(PUBLIC_END_DATETIME) as PUBLIC_END_DATETIME
from sakuhin_public_status sps
left join sakuhin s using(sakuhin_id)
where PUBLIC_START_DATETIME < now()
and PUBLIC_END_DATETIME > now()
and PUBLIC_END_DATETIME < now() + interval 7 day
GROUP BY 1,2