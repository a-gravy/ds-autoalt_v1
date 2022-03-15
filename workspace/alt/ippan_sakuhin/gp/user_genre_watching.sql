with D as (
    select
        user_multi_account_id,
        main_genre_code,
        sum(playback_time) as total_time
    from mv_movie_play mmp
    where playback_start_time >= now()- interval '30 day'
    and sakuhin_public_code like 'SID%'
    and main_genre_code not in ('SEMIADULT','ADULT','MUSIC_IDOL')
    and playback_time >= 120
group by grouping sets (1,(1,2))
    )
        , E as
    (
select
    user_multi_account_id,
    coalesce(main_genre_code,'overall') as main_genre_code,
    total_time
from D
    )
select
    user_multi_account_id,
    array_to_string(array_agg(main_genre_code order by total_time desc),'|') as main_genres,
    array_to_string(array_agg(total_time / (1.0 * total_wouf)*100 order by total_time desc),'|') as ratio
from E
         left join (
    select
        user_multi_account_id,
        total_time as total_wouf
    from E
    where main_genre_code = 'overall'
) a using(user_multi_account_id)
where total_time / (1.0 * total_wouf) * 100 >= 1
  and main_genre_code != 'overall'
group by 1