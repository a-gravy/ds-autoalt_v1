select
    sakuhin_public_code,
    sakuhin_name,
    sum(uu) as uu,
    row_number() over (order by sum(uu) desc) as today_top_rank
from datamart.agg_content_evaluation
where dt > now() - interval '2 day' -- '2021-04-14'
and main_genre_code not in ('SEMIADULT','ADULT','MUSIC_IDOL')
group by 1,2
limit 10
