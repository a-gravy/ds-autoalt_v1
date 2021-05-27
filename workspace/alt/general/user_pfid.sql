select
  pfid_matsubi,
  user_platform_id,
  user_multi_account_id
from dim_user_contract
left join (
  select
    substring(user_platform_id from char_length(user_platform_id)) as pfid_matsubi,
    user_platform_id,
    user_multi_account_id
  from (
    select *
    from dim_user_account
    where platform_public_code = 'unext'
    and delete_flg = 0
  ) dua
  where mod(substring(user_platform_id from char_length(user_platform_id))::int,2) = 0  -- even pfid
) a using(user_platform_id)
where status = 200
and a.user_multi_account_id is not null