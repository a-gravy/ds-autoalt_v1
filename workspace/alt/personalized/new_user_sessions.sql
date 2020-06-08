select
  a.userid,
  group_concat(a.sakuhin_code order by a.windowstart desc) as sids,
  group_concat(ROUND(a.windowlength) order by a.windowstart desc) as watched_time
  -- group_concat(a.windowstart order by a.windowstart desc) as windowstart
from recodb.fact_user_event_play as a
where a.windowlength > 2 and a.userid != '' and a.windowstart > now() + interval -1 day
group by a.userid