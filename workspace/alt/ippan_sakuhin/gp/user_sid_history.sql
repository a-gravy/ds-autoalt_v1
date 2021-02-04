select
user_multi_account_id,
array_to_string(array_agg(sakuhin_public_code order by playback_start_time  desc),'|') as sids
from mv_movie_play
where playback_start_time  >  now() - INTERVAL '30 day'
and sakuhin_public_code like 'SID%'
and playback_time > 120
group by user_multi_account_id