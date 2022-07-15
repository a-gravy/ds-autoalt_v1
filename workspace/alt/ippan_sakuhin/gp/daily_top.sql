select sakuhin_public_code,
       sakuhin_name,
       sum(uu) as   uu,
       row_number() over (order by sum(uu) desc) as today_top_rank
from datamart.agg_content_evaluation
where dt = (select max(dt) as dt from agg_content_evaluation)
  and main_genre_code not in (''SEMIADULT'', ''ADULT'', ''MUSIC_IDOL'')
group by 1, 2