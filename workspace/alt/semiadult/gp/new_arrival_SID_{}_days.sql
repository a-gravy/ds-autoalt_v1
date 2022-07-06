select
ds.sakuhin_public_code
from public.dim_sakuhin ds
	inner join public.dim_product dp on ds.sakuhin_public_code = dp.sakuhin_public_code
where ds.first_sale_datetime > now() - INTERVAL '7 day'
	and dp.main_genre_code = 'SEMIADULT'
	and ds.first_sale_datetime <= now()
order by ds.first_sale_datetime desc