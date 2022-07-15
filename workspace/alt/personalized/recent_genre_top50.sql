with recent_sakuhin as (
  SELECT
    distinct
    sakuhin_public_code,
    production_year,
    main_genre_code,
    sale_start_datetime,
    row_number() over (partition by main_genre_code order by sale_start_datetime desc) as recency
  FROM (
    select
      distinct
     sakuhin_public_code,
     main_genre_code,
     production_year,
     min(sale_start_datetime) as sale_start_datetime
   from dim_product
   WHERE sale_start_datetime < now()
   AND main_genre_code <> 'ADULT'
   and production_year is not null
   and production_year > extract('year' from now() - interval '4 year')
   group by 1,2,3
   ) P
)
, recent_sakuhin_score as 
(
  select
    rc.sakuhin_public_code,
    sum(first_touch_user) as ftu,
    sum(uu) as uu
  from datamart.agg_content_evaluation ace
  inner join recent_sakuhin rc using(sakuhin_public_code)
  where recency <= 50
  and dt > now() - interval '7 day'
  group by 1
)
select
  sakuhin_public_code,
  production_year,
  main_genre_code,
  sale_start_datetime,
  recency,
  coalesce(ftu,0) as ftu,
  coalesce(uu,0) as uu
from recent_sakuhin
left join recent_sakuhin_score using(sakuhin_public_code)
where recency <= 50
order by main_genre_code desc, recency asc
