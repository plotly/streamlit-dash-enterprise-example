import os

import pandas as pd
import pyarrow as pa
from databricks import sql


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

    def get_all_categories(self) -> list[str]:
        with self._connection.cursor() as cursor:
            cursor.execute(f"select distinct explode(fsq_category_labels[0]) FROM {self.source_table}")
            pa_table: pa.Table = cursor.fetchall_arrow()
            return pa_table.to_pandas().squeeze().dropna().to_list()

    def get_top_categories(self, n=50) -> pd.DataFrame:
        with self._connection.cursor() as cursor:
            cursor.execute(
                f"""
                select category, count(1) as cnt
                from (
                select explode(fsq_category_labels[0]) as category
                FROM {self.source_table}
                ) group by 1 order by 2 desc limit {n}
                """
            )
            pa_table: pa.Table = cursor.fetchall_arrow()
            return pa_table.to_pandas()

    def get_selected_categories(self, categories: list[str]) -> pd.DataFrame:
        with self._connection.cursor() as cursor:
            filter_statement = ",".join([f"'{c}'" for c in categories])

            cursor.execute(
                f"""select * FROM {self.source_table}
                           where size(array_intersect(fsq_category_labels[0], array({filter_statement}))) > 0
                           """
            )
            pa_table: pa.Table = cursor.fetchall_arrow()
            return pa_table.to_pandas()

    def get_top_associated(self, categories: list[str], n=50) -> pd.DataFrame:
        with self._connection.cursor() as cursor:
            filter_statement = ",".join([f"'{c}'" for c in categories])

            cursor.execute(
                f"""select category, count(1) as cnt
                from (
                select explode(fsq_category_labels[0]) as category
                FROM {self.source_table}
                where size(array_intersect(fsq_category_labels[0], array({filter_statement}))) > 0
                )
                where category not in ({filter_statement})
                group by 1 order by 2 desc limit {n}
                """
            )
            pa_table: pa.Table = cursor.fetchall_arrow()
            return pa_table.to_pandas()
