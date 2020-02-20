with recent_sakuhin as (
  select
   sakuhin_public_code,
   production_year,
   main_genre_code,
   min(sale_start_datetime) as sale_start_datetime
  FROM dim_product
  WHERE sale_start_datetime < now()
  AND sale_start_datetime > now() - INTERVAL '30 day' AND sale_end_datetime > now()
  AND production_year > EXTRACT ( YEAR FROM now() - INTERVAL '3 year')
  AND main_genre_code <> 'ADULT'
  group by 1,2,3
)
, recent_sakuhin_score as 
(
  select
    rc.sakuhin_public_code,
    sum(first_touch_user) as ftu,
    sum(uu) as uu
  from datamart.agg_content_evaluation ace
  inner join recent_sakuhin rc using(sakuhin_public_code)
  where dt > now() - interval '7 day'
  group by 1
)
select
  sakuhin_public_code,
  production_year,
  main_genre_code,
  sale_start_datetime,
  coalesce(ftu,0) as ftu,
  coalesce(uu,0) as uu
from recent_sakuhin
left join recent_sakuhin_score using(sakuhin_public_code)
order by sale_start_datetime desc
