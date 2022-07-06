with recent_sakuhin as (
SELECT
  DISTINCT sakuhin_public_code
FROM dim_product
WHERE sale_start_datetime < now()
AND sale_end_datetime > now()
AND main_genre_code <> 'ADULT'
)
, recent_sakuhin_score as 
(
  select
    rc.sakuhin_public_code,
    main_genre_code,
    avg(uu) as uu
  from datamart.agg_content_evaluation ace
  inner join recent_sakuhin rc using(sakuhin_public_code)
  where dt > now() - interval '7 day'
  group by 1,2
)
, ranked_sakuhin_score as
(
select
sakuhin_public_code,
main_genre_code,
uu,
row_number() over (partition by main_genre_code order by uu desc)         as row_num
from recent_sakuhin_score
)
select
rss.main_genre_code,
array_to_string(array_agg(sakuhin_public_code order by row_num asc),'|')  as sakuhin_codes,
array_to_string(array_agg(uu order by row_num asc),'|')                   as sakuhin_scores
from recent_sakuhin
inner join ranked_sakuhin_score rss using(sakuhin_public_code)
left join dim_sakuhin using(sakuhin_public_code)
where row_num <= 30
group by 1
