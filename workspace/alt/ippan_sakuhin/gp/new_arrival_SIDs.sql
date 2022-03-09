select
    sakuhin_public_code,
    display_name,
    main_genre_code
from public.dim_sakuhin
where first_sale_datetime > now() - INTERVAL '14 day'
  and first_sale_datetime <= now()
  and sakuhin_public_code like 'SID%'
  and main_genre_code not in ('SEMIADULT','ADULT','MUSIC_IDOL')
  and film_rating_code != 'R18'
order by first_sale_datetime desc