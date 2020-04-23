-- tmp workaround == get entire user sessions as new_user_sessions
select
  a.userid,
  group_concat(b.sakuhin_code order by b.windowstart desc) as sids,
  group_concat(b.windowlength order by b.windowstart desc) as watched_time
from recodb.fact_user_event_play as a
inner join recodb.fact_user_event_play as b
on (a.userid = b.userid and b.windowlength > 2)  -- and b.windowstart > now() + interval -1 day
group by a.userid
limit 300