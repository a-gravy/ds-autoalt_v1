select
    sess.userid,
    group_concat(sakuhin.SAKUHIN_PUBLIC_CODE order by sess.windowstart desc) as sakuhin_public_code,
    group_concat(sess.episode_code order by sess.windowstart desc)           as episode_code,
    group_concat(round(sess.windowlength, 2) order by sess.windowstart desc) as watched_time
from recodb.fues_dim_user_playback_session sess
         inner join cmsdb.episode ep on sess.episode_code = ep.EPISODE_PUBLIC_CODE
         inner join cmsdb.sakuhin sakuhin on ep.SAKUHIN_ID = sakuhin.SAKUHIN_ID
where sess.windowstart > now() - interval {} day
  and sess.windowlength > 2
  and sess.userid is not null
group by sess.userid;