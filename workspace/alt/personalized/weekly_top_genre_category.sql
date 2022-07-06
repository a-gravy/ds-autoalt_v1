select
  sakuhin_public_code,
  dsc.sakuhin_name,
  dsc.main_genre_code,
  category_name,
  sum(first_touch_user) as ftu,
 sum(uu)               as uu
from alt.dim_sakuhin_category dsc
left join datamart.agg_content_evaluation using(sakuhin_public_code)
where dt > now() - interval '7 day'
and dt < now() - interval '0 day'
and sakuhin_public_code like 'SID%'
group by 1,2,3,4
order by main_genre_code desc, ftu desc
