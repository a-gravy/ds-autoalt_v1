with candidates as (
select
distinct
sakuhin_public_code,
case when count(distinct episode_public_code) = 1 then 'vanilla' else 'fukusuu' end as is_one_sakuhin
from dim_product dp
WHERE main_genre_code <> 'ADULT'
and main_genre_code <> 'VARIETY'
and sale_type_code in ('TOD','SOD')
group by 1
order by sakuhin_public_code asc
)
, in_the_future as (
select
sakuhin_public_code,
dp.sale_start_datetime,
dp.sale_end_datetime,
first_value(dp.sale_start_datetime ) over (partition by sakuhin_public_code order by dp.sale_end_datetime desc) as next_start,
lag(dp.sale_start_datetime ) over (partition by sakuhin_public_code order by dp.sale_end_datetime desc) as lagged
from candidates
inner join dim_product dp using(sakuhin_public_code)
where is_one_sakuhin = 'vanilla'
), vanilla_values as (
select
distinct
sakuhin_public_code,
sale_start_datetime,
sale_end_datetime
--next_start - sale_end_datetime ,
--lagged - sale_end_datetime as next_sale
from in_the_future
where sale_start_datetime < now()
AND sale_end_datetime < now() + INTERVAL '7 day' AND sale_end_datetime > now()
and (lagged - sale_end_datetime != '00:00:01' and  next_start - sale_end_datetime != '00:00:01' or lagged is null)
)
, recent_sakuhin_score as 
(
  select
    rc.sakuhin_public_code,
    sum(first_touch_user) as ftu,
    sum(uu) as uu
  from datamart.agg_content_evaluation ace
  inner join vanilla_values rc using(sakuhin_public_code)
  where dt > now() - interval '7 day'
  group by 1
)
select
  distinct
  sakuhin_public_code,
  display_name as sakuhin_name,
  main_genre_code,
  coalesce(uu,0) as uu,
  coalesce(ftu,0) as ftu
from vanilla_values
left join recent_sakuhin_score using(sakuhin_public_code)
left join dim_sakuhin using(sakuhin_public_code)
order by uu desc
