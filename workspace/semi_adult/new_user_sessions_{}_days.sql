select
    sess.userid,
    group_concat(dp.SAKUHIN_PUBLIC_CODE order by sess.windowstart desc) as sakuhin_public_code,
    group_concat(sess.episode_code order by sess.windowstart desc)           as episode_code,
    group_concat(round(sess.windowlength, 2) order by sess.windowstart desc) as watched_time
from recodb.fues_dim_user_playback_session sess
         inner join recodb.dim_product dp on sess.episode_code = dp.episode_public_code
where sess.windowstart > now() - interval {} day
  and sess.windowlength > 2
  and sess.userid is not null
  and dp.main_genre_code = 'SEMIADULT'
group by sess.userid;