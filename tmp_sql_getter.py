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



def main():
    # get_expire_soon()
    get_alt_definition()


if __name__ == '__main__':
    main()


