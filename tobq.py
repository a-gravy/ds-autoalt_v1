import logging
import pandas
import pandas as pd
from google.cloud import bigquery

logger = logging.getLogger(__name__)


class BigQuery:
    def __init__(self):
        self.bq_client = bigquery.Client()

    def upload_csv(self, csv_path, table_id, string_cols=[], parse_date_cols=[]):
        df = pd.read_csv(csv_path, parse_dates=parse_date_cols)
        print(df)
        print("Pandas data types")
        print(df.dtypes)
        self.upload_df(df, table_id, string_cols)

    def upload_df(self, df, table_id, string_cols=[]):

        schema = [
            bigquery.SchemaField(string_col, bigquery.enums.SqlTypeNames.STRING)
            for string_col in string_cols
        ]

        job_config = bigquery.LoadJobConfig(
            schema=schema,
        )

        job = self.bq_client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )  # Make an API request.
        job.result()  # Wait for the job to complete.

        table = self.bq_client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    def download_df(self, query_string):

        dataframe = (
            self.bq_client.query(query_string)
                .result()
                .to_dataframe(
            )
        )
        return


if __name__ == '__main__':
    bq = BigQuery()
    bq.upload_csv(csv_path="non_acc_metrics.csv",
                  table_id="un-ds-dwh.model_metrics.ippan_non_accuracy_metrics")
