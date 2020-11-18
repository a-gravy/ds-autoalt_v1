select
ds.sakuhin_public_code
from recodb.dim_sakuhin ds
	inner join dim_product dp on ds.sakuhin_public_code = dp.sakuhin_public_code
where ds.first_sale_datetime > now() - INTERVAL {} day
	and dp.main_genre_code = 'SEMIADULT'
	and ds.first_sale_datetime <= now()
order by ds.first_sale_datetime desc