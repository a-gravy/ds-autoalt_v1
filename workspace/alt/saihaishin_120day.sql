/* 再配信作品 */
with recent_sakuhin as (
  select
    sakuhin_public_code,
    production_year,
    main_genre_code,
    min(sale_start_datetime) as sale_start_datetime,
    previous_sale_start_datetime,
    recency
  FROM (
    select
      sakuhin_public_code,
      production_year,
      main_genre_code,
      sale_end_datetime,
      min(sale_start_datetime)                                                                          as sale_start_datetime,
      min(sale_end_datetime) over (partition by sakuhin_public_code)                                    as recency,
      row_number() over (partition by sakuhin_public_code order by sale_start_datetime desc)            as row_num,
      lead(sale_end_datetime) over (partition by sakuhin_public_code order by sale_end_datetime desc)   as previous_sale_start_datetime
    FROM dim_product
    WHERE main_genre_code <> 'ADULT'
    group by 1,2,3, sale_end_datetime, sale_start_datetime
    ) P 
  WHERE sale_start_datetime < now()
  AND sale_start_datetime > now() - INTERVAL '120 day' AND sale_end_datetime > now()
  AND production_year > EXTRACT ( YEAR FROM now() - INTERVAL '3 year')
  AND main_genre_code <> 'ADULT'
  and row_num = 1 and sale_start_datetime - previous_sale_start_datetime > interval '30 day'
  group by 1,2,3, recency, sale_end_datetime, previous_sale_start_datetime
)
select
  sakuhin_public_code ,
  display_name             as sakuhin_name,
  ds.production_year,
  rs.main_genre_code,
  max(uu)                  as uu,
  max(first_touch_user)    as ftu,
  previous_sale_start_datetime	
from datamart.agg_content_evaluation ace 
inner join recent_sakuhin rs using(sakuhin_public_code)
inner join dim_sakuhin ds using(sakuhin_public_code)
where recency < now() - interval '90 day'
group by 1,2,3,4, recency, previous_sale_start_datetime
