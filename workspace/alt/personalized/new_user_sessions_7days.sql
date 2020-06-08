select
  a.userid,
  group_concat(a.sakuhin_code order by a.windowstart desc) as sids,
  group_concat(a.episode_code order by a.windowstart desc) as eps,
  group_concat(ROUND(a.windowlength) order by a.windowstart desc) as watched_time
from recodb.fact_user_event_play as a
where a.windowlength > 2 and a.userid != '' and a.windowstart > now() + interval -7 day
group by a.userid