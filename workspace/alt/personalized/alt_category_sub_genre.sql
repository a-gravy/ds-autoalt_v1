select
alt_public_code,
main_genre_code,
category_name,
sakuhin_public_code
from alt.dim_sakuhin_category
inner join alt.dim_alt_category using(main_genre_code,category_name)
order by alt_public_code asc
