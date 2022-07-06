with datas as (
  select
    main_genre_code,
    sakuhin_public_code,
    sakuhin_name,
    row_number() over (partition by main_genre_code order by sum(uu) desc) as row_th
  from datamart.agg_content_evaluation
  --where dt >= now() - interval '30 day'
  where main_genre_code not in ('SEMIADULT','ADULT','MUSIC_IDOL')
  group by 1,2,3
)
select
*
from datas
where row_th <= 100