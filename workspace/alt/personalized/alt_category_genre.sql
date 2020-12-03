select
distinct
alt_public_code,
main_genre_code,
sakuhin_public_code
from alt.dim_sakuhin_category
inner join alt.dim_alt_category dac using(main_genre_code)
where dac.category_name is null
order by alt_public_code asc
