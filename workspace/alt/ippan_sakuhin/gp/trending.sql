with todays_data as (
  select
    sakuhin_public_code,
    sakuhin_name,
    sum(uu) as uu,
    row_number() over (order by sum(uu) desc) as today_top_rank
  from datamart.agg_content_evaluation
  where dt > now() - interval '2 day' -- '2021-04-14'
  -- and sakuhin_public_code like 'SID%'
  and main_genre_code not in ('SEMIADULT','ADULT','MUSIC_IDOL')
  group by 1,2
)
, zenkai_data as
(
  select
    sakuhin_public_code,
    sum(uu) as zenkai_uu
  from datamart.agg_content_evaluation
  where dt > now() - interval '3 day' and dt < now() - interval '2 day' -- date('2021-04-14') - interval '1 day'
  and sakuhin_public_code like 'SID%'
  group by 1
)
select
  today_top_rank,
  sakuhin_public_code,
  sakuhin_name,
  uu,
  coalesce(zenkai_uu,1) as zenkai_uu,
  uu / (1.0 * coalesce(zenkai_uu,1)) as trend_perc
from todays_data
left join zenkai_data using(sakuhin_public_code)
where today_top_rank > 10 and today_top_rank <= 500
order by 6 desc