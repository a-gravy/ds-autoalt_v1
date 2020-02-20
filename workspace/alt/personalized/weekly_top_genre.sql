select
  sakuhin_public_code,
  sakuhin_name,
  main_genre_code,
  sum(first_touch_user) as ftu,
  sum(uu)               as uu
from datamart.agg_content_evaluation 
where dt > now() - interval '7 day'
and dt < now() - interval '0 day'
and sakuhin_public_code like 'SID%'
group by 1,2,3
order by main_genre_code desc, ftu desc
