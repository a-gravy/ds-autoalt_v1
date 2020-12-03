select
  mpl.user_multi_account_id,
  ARRAY_JOIN(array_agg(mpl.sakuhin_code order by mpl.playback_start_time desc ), '|') as sakuhin_public_code,
  ARRAY_JOIN(array_agg(mpl.episode_code order by mpl.playback_start_time desc ), '|') as episode_code,
  ARRAY_JOIN(array_agg(mpl.playback_time order by mpl.playback_start_time desc ), '|') as watched_time
from movie_play_log mpl
where TD_TIME_RANGE(mpl.time,TD_TIME_ADD(cast(now() as varchar),'-7d'), null, 'JST')
and mpl.sakuhin_code like 'SID%'
and mpl.playback_time >= 1200
group by mpl.user_multi_account_id