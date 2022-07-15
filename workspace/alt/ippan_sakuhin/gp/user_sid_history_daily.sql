select
user_multi_account_id,
array_to_string(array_agg(sakuhin_public_code order by playback_start_time  desc),'|') as sids
from mv_movie_play
where playback_start_time  >  now() - INTERVAL '1 day'
and main_genre_code not in ('SEMIADULT','ADULT','MUSIC_IDOL')
and playback_time > 120
group by user_multi_account_id