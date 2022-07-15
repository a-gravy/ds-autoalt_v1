select
sas.anikore_id
from kensaku.dim_anikore sas
left join kensaku.anikore_sakuhin_rel asr
on sas.anikore_id = asr.anikore_id::int
where asr.sakuhin_public_code is not null
order by user_number_rating desc
limit 500
