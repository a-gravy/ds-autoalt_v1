"""
this is tmp place to execute sql and put data to s3,
will be removed after migrating to DAG
-----
ecent_30day are the most recent sakuhin

rence_genre_top_50 are the most top 50 recent sakuhin for each genre
5:14
saishaishin 120day is the sakuhin that were on unext before and are getting aired recently (within 120 days)
5:14
weekly_top_genre is the weekly top sakuhin for each genre
5:15
weekly_top_genre_category is the weekly top sakuhin for each genre且つcategory
"""
from pathlib import Path
from dstools.utils import get_filepath_content, send_file_to_dest
from dstools.connectors.postgres import Query as PostgresQuery
from dstools.connectors.s3 import S3

dw_conn_string = "postgres://reco_etl:recoreco@10.232.201.241:5432/unext_analytics_dw?charset=utf8"


def get_newarrival():
    in_path = str(Path("workspace/alt/recent_30day.sql"))
    in_sql = get_filepath_content(in_path)
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "new_arrival.csv", True)
    #S3.upload_file("watched_list_ippan.csv", s3_root_location + "watched_list_ippan.csv", zipped=True,
    #               delete_source_file=True)


def get_newarrival_by_genre():
    in_path = str(Path("workspace/alt/recent_genre_top50.sql"))
    in_sql = get_filepath_content(in_path)
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "new_arrival_genre.csv", True)


def get_trending():
    in_path = str(Path("workspace/alt/trending_genre_top66uu.sql"))
    in_sql = get_filepath_content(in_path)
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "trending.csv", True)


def get_top():
    """
    weekly_top_genre contains lot of sakuhins, need to pick top N for each type later
    """
    in_path = str(Path("workspace/alt/weekly_top_genre.sql"))
    in_sql = get_filepath_content(in_path)
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "weekly_top.csv", True)


def get_expire_soon():
    in_path = str(Path("workspace/alt/personalized/expire_soon.sql"))
    in_sql = get_filepath_content(in_path)
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "expire_soon.csv", True)


def get_alt_definition():
    in_sql = """
    select * from alt.dim_alt 
    order by alt_public_code asc
    """
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "alt_definition.csv", True)


def get_target_users():
    in_sql = """
        select
        distinct
        user_multi_account_id
        from alt.target_users
        """
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "target_users.csv", True)
    # TODO: remember to skip the header while using file_to_list


def get_lastest_watched_sakuhins():   # for demo
    in_sql = """
    with user_data as (
    select
    user_multi_account_id,
    sakuhin_public_code,
    playback_start_time
    from alt.target_users tu 
    inner join (
      select
      *
      from mv_movie_play mmp 
      where playback_start_time > now() - interval '90 day'
      and sakuhin_public_code like 'SID%'
      and playback_time >= 120
    ) df using(user_multi_account_id)
    ), user_rank as (
    select
    user_multi_account_id,
    sakuhin_public_code,
    rank() over (partition by user_multi_account_id order by playback_start_time desc) as ranking
    from user_data
    group by 1,2, playback_start_time
    )
    select
    user_multi_account_id,
    sakuhin_public_code
    from user_rank
    where ranking = 1
    """
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "last_watched_list.csv", True)

    in_sql = """
    select sakuhin_public_code, display_name
    from public.dim_sakuhin
    where sakuhin_public_code like 'SID%'
    """
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "sakuhin_meta.csv", True)

    import pandas as pd
    last = pd.read_csv("last_watched_list.csv")
    meta = pd.read_csv("sakuhin_meta.csv")
    m = pd.merge(last, meta, how='left', on='sakuhin_public_code')
    m.to_csv("last_watched_alt.csv", index=False)


def demo_push_2_dim_alt():
    import psycopg2
    conn = psycopg2.connect(database="unext_analytics_dw", user="reco_etl", password="recoreco", host="10.232.201.241",
                            port="5432")
    cursor = conn.cursor()
    with open("../ds-ippan-recommender/alts/demo_user_insert_alts.sql", 'r') as f:
        content = f.read()

    cursor.execute(content)
    conn.commit()
    conn.close()


def demo_get_demo_user_history():
    in_sql = """
    select
    user_multi_account_id,
    array_to_string(array_agg(substring(sakuhin_public_code,4)::int order by playback_start_time  desc),'|') as sakuhin_ids,
    array_to_string(array_agg(playback_time order by playback_start_time  desc),'|') as playback_times,
    array_to_string(array_agg(coalesce(play_time,0) order by playback_start_time  desc),'|') as play_times
    from mv_movie_play_1_prt_ay2020 mmppym 
    inner join (
	    select * from alt.target_users
    ) tu using(user_multi_account_id)
    where sakuhin_public_code like 'SID%'
    group by 1
    --limit 100
    """
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "../user_watch_history/demo_user_history_2020.csv", True)


def demo_get_dim_table():
    """

    :return:
    """
    in_sql = """
    select
    alt_public_code  as public_code,
    alt_name as title,
    alt_description as description
    from alt.dim_alt da
    """
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "auto_alt_meta.csv", True)


def main():
    # get_expire_soon()
    demo_get_dim_table()


if __name__ == '__main__':
    main()


