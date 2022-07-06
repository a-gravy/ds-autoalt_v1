select
    sess.episode_code,
    group_concat(DISTINCT sakuhin.SAKUHIN_PUBLIC_CODE) as SID,
    group_concat(DISTINCT sakuhin.DISPLAY_NAME) as display_name,
    group_concat(DISTINCT sakuhin.MAIN_GENRE_CODE) as main_genre_code,
    COUNT(DISTINCT sess.userid) as nb_watch
from recodb.fues_dim_user_playback_session sess
         inner join cmsdb.episode ep on sess.episode_code = ep.EPISODE_PUBLIC_CODE
         inner join cmsdb.sakuhin sakuhin on ep.SAKUHIN_ID = sakuhin.SAKUHIN_ID
where sess.windowstart > now() - interval {} day
  and sess.windowlength > 2
group by sess.episode_code
order by nb_watch desc
limit 300;