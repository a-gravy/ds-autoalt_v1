select
sakuhin_public_code
,display_name
,MAIN_GENRE_CODE
,sd.DISPLAY_PRODUCTION_COUNTRY
,sp.POPULARITY_POINT
,exclusive_start_datetime
,exclusive_end_datetime
,exclusive_type_code
,exclusive_type_name
from sakuhin
inner join sakuhin_exclusive_badge as seb using(sakuhin_id)
inner join cls_exclusive_type using(exclusive_type_code)
inner join sakuhin_platform as sp using (sakuhin_id)
inner join sakuhin_detail sd using (sakuhin_id)
where seb.PLATFORM_ID = 1
and sp.PLATFORM_ID = 1
and exclusive_start_datetime < now()
and exclusive_end_datetime > now()
order by MAIN_GENRE_CODE, sp.POPULARITY_POINT desc
