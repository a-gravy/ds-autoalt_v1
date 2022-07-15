select
ct.person_name_id,
array_to_string(array_agg(distinct dc.person_name),'|') as name,
array_to_string(array_agg(distinct ct.sakuhin_public_code),'|') as sids,
array_to_string(array_agg(ct.cast_type_name),'|') as roles
from staging.dim_cast_episode_rel as ct
inner join staging.dim_cast as dc
on ct.person_name_id = dc.person_name_id
inner join public.dim_product as dp
on ct.sakuhin_public_code = dp.sakuhin_public_code
where dp.sakuhin_public_code like 'SID%'
and dp.sale_start_datetime < now()   -- filter out unavailable sakuhins, so that reco won't have them
and dp.sale_end_datetime >= now()
and dc.is_adult = 0  -- ippan & adult share one person_name_id
group by 1