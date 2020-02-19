with current_sakuhin as (
select
  distinct
  sakuhin_public_code
from dim_product
inner join dim_sakuhin ds using(sakuhin_public_code)
WHERE sale_start_datetime < now()
AND ds.main_genre_code <> 'ADULT'
and film_rating_code != 'R18'
), top_sakuhins as (
select
  main_genre_code,
  avg(uu) as uu,
  stddev(uu) as stdu,
  avg(uu) + (1 * stddev(uu)) as top66
from datamart.agg_content_evaluation 
where dt > now() - interval '7 day'
and sakuhin_public_code like 'SID%'
group by 1
), prev_week_score as (
select
  rc.sakuhin_public_code,
  max(first_touch_user) as ftu,
  max(uu) as uu
from datamart.agg_content_evaluation ace
inner join current_sakuhin rc using(sakuhin_public_code)
where dt > now() - interval '14 day'
and dt < now() - interval '7 day'
group by 1
), curr_week_score as (
select
  rc.sakuhin_public_code,
  max(first_touch_user) as ftu,
  max(uu) as uu
from datamart.agg_content_evaluation ace
inner join current_sakuhin rc using(sakuhin_public_code)
where dt > now() - interval '7 day'
group by 1
)
select
cws.uu / pws.uu::float * 100 as uu_improve,
cws.uu,
ds.sakuhin_public_code,
ds.main_genre_code,
ds.display_name
from current_sakuhin
left join prev_week_score pws using(sakuhin_public_code)
left join curr_week_score cws using(sakuhin_public_code)
left join dim_sakuhin ds using(sakuhin_public_code)
left join top_sakuhins using(main_genre_code)
where pws.uu is not null
and cws.uu is not null
and cws.uu >= top66
order by main_genre_code asc, uu_improve desc
