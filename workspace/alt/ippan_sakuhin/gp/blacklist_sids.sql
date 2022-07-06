select  -- eroi sakuhins
  sakuhin_public_code
FROM dim_cms_ero
union
select  -- R18 & semiadult
  distinct
  sakuhin_public_code
from dim_sakuhin
where dim_sakuhin.film_rating_code = 'R18'
or dim_sakuhin.main_genre_code = 'SEMIADULT'
union
select  -- NHK sakuhins
  distinct
  sakuhin_public_code
from dim_product ds
where content_holder_public_code = 	'hol0001724'
and sale_end_datetime > now()