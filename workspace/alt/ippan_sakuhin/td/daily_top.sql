select
  episode_code,
  mpl.sakuhin_code as SID,
  mpl.sakuhin_name as display_name,
  cs.MAIN_GENRE_CODE as main_genre_code,
  count(distinct user_multi_account_id) as nb_watch
from movie_play_log mpl
left join unext_basis_log_db.cms_sakuhin cs
on mpl.sakuhin_code = cs.SAKUHIN_PUBLIC_CODE
where TD_TIME_RANGE(mpl.time,TD_TIME_ADD(cast(now() as varchar),'-1d'), null, 'JST')
and mpl.sakuhin_code like 'SID%'
group by 1,2,3,4
order by 5 desc
limit 300;