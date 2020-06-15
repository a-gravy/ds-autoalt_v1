select
  sakuhin_public_code,
  total_time_watched
from agg_content_evaluation
where dt >= now() - interval '2 day'  -- since currently we use yesterday
and sakuhin_public_code like 'SID%'
order by total_time_watched desc
limit 30