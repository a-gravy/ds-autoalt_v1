select
	sakuhin_public_code
from public.dim_sakuhin
where first_sale_datetime > now() - INTERVAL '{} day'
and first_sale_datetime <= now()
and sakuhin_public_code like 'SID%'
order by first_sale_datetime desc
