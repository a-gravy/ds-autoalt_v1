with main_code as (
  select
    distinct menu_public_code
  from dim_sakuhin_menu
  where parent_menu_public_code is null
), tags as (
  select
    distinct
    parent_menu_public_code,
    parent_menu_name,
    dsm.menu_public_code,
    menu_name
  from dim_sakuhin_menu dsm
  inner join main_code mc
  on dsm.parent_menu_public_code = mc.menu_public_code
  where menu_name not like '%%ランキング%%'
  and menu_name not like '%%作品%%'
  and menu_name not like '%%歳%%'
  and menu_name not like '%%行'
  and parent_menu_public_code in (
  'MNU0000001',
  'MNU0000018',
  'MNU0000035',
  'MNU0000050',
  'MNU0000063',
  'MNU0000076',
  'MNU0000090',
  'MNU0000102',
  'MNU0000117',
  'MNU0000124'
))
select
  dm.sakuhin_public_code,
  dm.display_name,
  dm.main_genre_code,
  -- tags.menu_public_code,
  array_to_string(array_agg(tags.menu_name),'|') as menu_names,
  -- tags.parent_menu_public_code,
  array_to_string(array_agg(tags.parent_menu_name), '|') as parent_menu_names,
  current_sakuhin.display_production_country as nations
from dim_sakuhin_menu
inner join tags using(menu_public_code)
right join (
  select
    distinct sakuhin_public_code,
    display_production_country
  from dim_product
--  where sale_end_datetime >= now()
--  and sale_start_datetime < now()
  ) current_sakuhin using(sakuhin_public_code)
inner join dim_sakuhin dm using(sakuhin_public_code)
group by dm.sakuhin_public_code, dm.display_name, dm.main_genre_code, nations
--where sakuhin_public_code = 'SID0002167'
order by sakuhin_public_code asc