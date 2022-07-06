select
    KIDS_SAKUHIN_GROUP_PUBLIC_CODE,
    KIDS_SAKUHIN_GROUP_NAME,
    group_concat(sakuhin_public_code separator '|') as SIDs
from kids_sakuhin_group ksg
         left join kids_sakuhin_group_sakuhin_rel ksgsr using(kids_sakuhin_group_id)
         left join kids_sakuhin ks using(kids_sakuhin_id)
         left join sakuhin using(sakuhin_id)
where ksg.DISPLAY_STATUS_CODE = 'PUBLIC'
  and PUBLIC_START_DATETIME < now()
  and PUBLIC_END_DATETIME > now() + interval 1 day
group by 1, 2;