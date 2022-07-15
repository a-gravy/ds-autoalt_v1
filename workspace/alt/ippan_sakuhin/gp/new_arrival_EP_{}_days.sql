select
sakuhin_public_code,
episode_public_code,
product_episode_code,
sakuhin_name,
episode_name,
episode_no,
sale_start_datetime
from public.dim_product
where sale_start_datetime > now() - INTERVAL '{} day'
and sale_start_datetime <= now()
and sakuhin_public_code like 'SID%'
and main_genre_code != 'SEMIADULT'
and main_genre_code != 'MUSIC_IDOL'
and main_genre_code != 'SPORT'
and main_genre_code != 'YOUGA'
and main_genre_code != 'HOUGA'
and main_genre_code != 'NEWS'
and main_genre_code != 'DOCUMENT'
order by episode_no