"""
this is tmp place to execute sql and put data to s3,
will be removed after migrating to DAG
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



def main():
    get_newarrival_by_genre()


if __name__ == '__main__':
    main()


