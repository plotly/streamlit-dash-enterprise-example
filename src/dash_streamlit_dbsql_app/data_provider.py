import os

import pandas as pd
from databricks import sql
import pyarrow as pa


class DataProvider:
    def __init__(self) -> None:
        self._connection = self._get_connection()

    def _get_connection(self):
        return sql.connect(
            server_hostname=os.getenv("DATABRICKS_SERVER_HOSTNAME"),
            http_path=os.getenv("DATABRICKS_HTTP_PATH"),
            access_token=os.getenv("DATABRICKS_TOKEN"),
            use_cloud_fetch=True,
        )

    @property
    def source_table(self) -> str:
        return f'{os.getenv("DATABRICKS_CATALOG")}.{os.getenv("DATABRICKS_SCHEMA")}.{os.getenv("DATABRICKS_TABLE")}'

    def get_categories(self) -> pd.DataFrame:
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"SELECT * FROM {os.getenv('DATABRICKS_CATALOG')}.{os.getenv('DATABRICKS_SCHEMA')}.{os.getenv('DATABRICKS_TABLE')} LIMIT 1000"
            )
            pa_table: pa.Table = cursor.fetchall_arrow()
            return pa_table.to_pandas()

    def get_all_categories(self) -> list[str]:
        with self._connection.cursor() as cursor:
            cursor.execute(f"select distinct explode(fsq_category_labels[0]) FROM {self.source_table}")
            pa_table: pa.Table = cursor.fetchall_arrow()
            return pa_table.to_pandas().squeeze().dropna().to_list()

    def get_top_categories(self, n=50) -> pd.DataFrame:
        with self._connection.cursor() as cursor:
            cursor.execute(
                f'select explode(fsq_category_labels[0]) as category, count(1) as cnt FROM {self.source_table} group by 1 order by 2 desc limit 50'
            )
            pa_table: pa.Table = cursor.fetchall_arrow()
            return pa_table.to_pandas()

    def get_selected_categories(self, categories: list[str]) -> pd.DataFrame:
        with self._connection.cursor() as cursor:
            filter_statement = ','.join([f"'{c}'" for c in categories])

            cursor.execute(
                f"""select * FROM {self.source_table}
                           where size(array_intersect(fsq_category_labels[0], array({filter_statement}))) > 0
                           """
            )
            pa_table: pa.Table = cursor.fetchall_arrow()
            return pa_table.to_pandas()

    def get_mapping(self) -> pd.DataFrame:
        with self._connection.cursor() as cursor:
            statement = f"""
                select explode(neighborhood),cl, count(distinct fsq_id) as cnt
                from (select *, explode(fsq_category_labels[0]) as cl from {self.source_table})
                group by 1,2
                order by 3 desc
            """
            cursor.execute(statement)
            pa_table: pa.Table = cursor.fetchall_arrow()
            return pa_table.to_pandas()