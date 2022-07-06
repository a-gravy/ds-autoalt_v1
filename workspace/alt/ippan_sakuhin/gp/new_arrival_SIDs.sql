select
    ds.sakuhin_public_code,
    ds.display_name,
    ds.main_genre_code,
    ds.production_year,
    sum(ace.uu) as   uu
from public.dim_sakuhin as ds
left join datamart.agg_content_evaluation ace on ds.sakuhin_public_code = ace.sakuhin_public_code
where ds.first_sale_datetime > now() - INTERVAL '14 day'
  and ds.first_sale_datetime <= now()
  and ds.sakuhin_public_code like 'SID%'
  and ds.main_genre_code not in ('SEMIADULT','ADULT','MUSIC_IDOL')
  and ds.film_rating_code != 'R18'
  and ds.production_year >=  date_part('year', now()) - 3
group by 1, 2, 3, 4
order by uu desc