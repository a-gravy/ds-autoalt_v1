select
  mpl.user_multi_account_id,
  ARRAY_JOIN(array_agg(mpl.sakuhin_code order by mpl.playback_start_time desc ), '|') as sakuhin_public_code,
  ARRAY_JOIN(array_agg(mpl.episode_code order by mpl.playback_start_time desc ), '|') as episode_code,
  ARRAY_JOIN(array_agg(mpl.playback_time order by mpl.playback_start_time desc ), '|') as watched_time
from movie_play_log mpl
left join unext_basis_log_db.cms_sakuhin cs on mpl.sakuhin_code = cs.SAKUHIN_PUBLIC_CODE
where TD_TIME_RANGE(mpl.time,TD_TIME_ADD(cast(now() as varchar),'-7d'), null, 'JST')
and cs.main_genre_code = 'SEMIADULT'
and mpl.playback_time >= 1200
group by mpl.user_multi_account_id