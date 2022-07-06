"""
this is tmp place to execute sql and put data to s3,
will be removed after migrating to DAG
-----
ecent_30day are the most recent sakuhin

"""
import tempfile
import os
from pathlib import Path
from dstools.utils import get_filepath_content, send_file_to_dest
from dstools.connectors.postgres import Query as PostgresQuery
from dstools.connectors.mysql import Query as mysql
from dstools.connectors.mysql import Query as MysqlQuery
from dstools.connectors.s3 import S3

dw_conn_string = "postgres://reco_etl:recoreco@10.232.201.241:5432/unext_analytics_dw?charset=utf8"
s3_root_location = "s3://unext-datascience/alts/"
cmsdb = 'mysql://cmsteam:Z1NwN6e0@10.105.22.207:3306/cmsdb?charset=utf8'

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
    weekly_top_genre contains lot of sakuhins, need to pick top N for each metric_name later
    """
    # in_path = str(Path("workspace/alt/weekly_top_genre.sql"))
    in_path = str(Path("workspace/alt/personalized/daily_top.sql"))
    in_sql = get_filepath_content(in_path)
    query = PostgresQuery(dw_conn_string)
    # query.to_csv(in_sql, "weekly_top.csv", True)
    query.to_csv(in_sql, "data/daily_top.csv", True)


def get_expire_soon():
    in_path = str(Path("workspace/alt/personalized/expire_soon.sql"))
    in_sql = get_filepath_content(in_path)
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "expire_soon.csv", True)


def get_alt_definition():
    in_sql = """
    select * from alt.dim_alt 
    order by ALT_code asc
    """
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "data/alt_definition.csv", True)


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
    ALT_code  as public_code,
    alt_name as title,
    alt_description as description
    from alt.dim_alt da
    """
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, "auto_alt_meta.csv", True)


def get_sth_postegres(task):
    # chose the _.sql in workspace/alt/personalized/
    # workspace/alt/ippan_sakuhin/gp/user_sid_history.sql
    # in_path = os.path.join(f"workspace/alt/ippan_sakuhin/gp/{task}.sql")
    in_path = os.path.join(f"workspace/alt/ippan_sakuhin/gp/{task}.sql")
    in_sql = get_filepath_content(in_path).format(14)
    print(in_sql)
    query = PostgresQuery(dw_conn_string)
    query.to_csv(in_sql, f"data/{task}.csv", True)


def get_sth_cmsdb(task):
    in_sql = get_filepath_content(str(Path(f"workspace/alt/ippan_sakuhin/cmsdb/{task}.sql")))
    query = mysql(cmsdb)
    query.to_csv(in_sql, "data/{}.csv".format(task), True)

workspace_root = "workspace"
def get_sth_mysql(**kwargs):
    in_path = str(Path(workspace_root, kwargs['sql']))
    in_sql = get_filepath_content(in_path)

    query = MysqlQuery(cmsdb)

    # _, tmp_file_path = tempfile.mkstemp(dir="/data")
    tmp_file_path = "tmp"
    query.to_csv(in_sql, tmp_file_path, with_header=True)
    S3.upload_file(tmp_file_path, f"{kwargs['s3_dir']}{kwargs['csv']}",
                   zipped=True, delete_source_file=True)



def get_sth_tidb():
    #tidb_conn_string = 'mysql://reco:reco@10.232.201.18:3306/searchenginedb?charset=utf8'  # version 3
    tidb_conn_string = 'mysql://reco:reco@10.232.201.38:3306/recodb?charset=utf8'  # version 4
    # task = "new_user_sessions_{}_days"
    task = "daily_top_genre"  # "daily_top" "new_user_sessions_{}_days"
    n_days = 1

    in_sql = get_filepath_content(str(Path("workspace/alt/personalized/{}.sql".format(task))))
    query = mysql(tidb_conn_string)
    preoperator = "set group_concat_max_len = 1024000;"  # 4294967295
    query.to_csv(in_sql.format(n_days), "data/{}.csv".format(task.format(n_days)), True, preoperator=preoperator)


def get_sth_semi_tidb():
    #tidb_conn_string = 'mysql://reco:reco@10.232.201.18:3306/searchenginedb?charset=utf8'  # version 3
    tidb_conn_string = 'mysql://reco:reco@10.232.201.38:3306/recodb?charset=utf8'  # version 4
    # task = "new_user_sessions_{}_days"
    task = "new_user_sessions_{}_days"  # "daily_top" "new_user_sessions_{}_days"
    n_days = 15

    in_sql = get_filepath_content(str(Path("workspace/semi_adult/{}.sql".format(task))))
    query = mysql(tidb_conn_string)
    preoperator = "set group_concat_max_len = 1024000;"  # 4294967295
    query.to_csv(in_sql.format(n_days), "data/semi_adult_{}.csv".format(task.format(n_days)), True, preoperator=preoperator)


def push_2_dw():
    #_, tmp_file_path = tempfile.mkstemp(dir="/data")
    #from_file = s3_root_location + f"anikore_sid_matching.csv.gz"
    #S3.download_file(from_file, tmp_file_path)

    query = PostgresQuery(dw_conn_string)
    #preoperator = "truncate table kensaku.dim_anikore_matching"
    query.upload_from_gzipped_csv("alt.dim_alt_n", "data/dim_alt_n.csv.gz", delete_source_file=True)


def main():
    # get_expire_soon()
    # get_sth_postegres("user_genre_watching")
    get_sth_cmsdb("kids_characters_meta")

    """
    kwargs={
        "s3_dir": "s3://unext-datascience/alts/ippan_sakuhin/",
        'sql': "alt/ippan_sakuhin/cmsdb/person_name_id_mapping.sql",
        'csv': "person_name_id_mapping.csv"
    }
    get_sth_mysql(**kwargs)
    """


if __name__ == '__main__':
    main()


