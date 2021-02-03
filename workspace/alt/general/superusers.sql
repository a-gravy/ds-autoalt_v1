select
distinct duc.user_multi_account_id
from dim_super_user dsu
inner join dim_user_account duc
on dsu.user_platform_id = duc.user_platform_id