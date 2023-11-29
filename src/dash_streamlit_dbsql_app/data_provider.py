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

    def get_categories_hierarchy(self, categories: list[str]) -> pd.DataFrame:
        with self._connection.cursor() as cursor:
            filter_statement = ",".join([f"'{c}'" for c in categories])
            cursor.execute(
                f"""SELECT * from (
                    WITH ExplodedCategories AS (
                        SELECT name, explode(fsq_category_labels[0]) AS category
                        FROM {self.source_table}
                    ),
                    AssociatedCategories AS (
                        SELECT 
                            ec1.category AS primary_category, 
                            ec2.category AS associated_category
                        FROM ExplodedCategories ec1
                        JOIN ExplodedCategories ec2 
                        ON ec1.name = ec2.name AND ec1.category != ec2.category
                    )
                    SELECT 
                        ec.category, 
                        COUNT(*) AS num_places_of_that_category,
                        ARRAY_AGG(DISTINCT ac.associated_category) FILTER (WHERE ac_rank <= 5) AS top_3_associated_categories_array
                    FROM 
                        ExplodedCategories ec
                    LEFT JOIN (
                        SELECT 
                            primary_category, 
                            associated_category,
                            RANK() OVER (PARTITION BY primary_category ORDER BY COUNT(*) DESC) AS ac_rank
                        FROM AssociatedCategories
                        GROUP BY primary_category, associated_category
                    ) ac ON ec.category = ac.primary_category
                    GROUP BY ec.category)
                    WHERE category in ({filter_statement})"""
            )
            pa_table: pa.Table = cursor.fetchall_arrow()
            return pa_table.to_pandas()

    def get_zip_codes(self, categories: list[str]) -> pd.DataFrame:
        with self._connection.cursor() as cursor:
            filter_statement = ",".join([f"'{c}'" for c in categories])
            cursor.execute(
                f"""SELECT postcode, SUM(count_places) as total_places
                    FROM (
                        SELECT postcode, count(*) as count_places
                        FROM (
                            SELECT explode(fsq_category_labels[0]) as category, postcode, name
                            FROM {self.source_table}
                        ) as exploded
                        WHERE category IN ({filter_statement})
                        GROUP BY postcode, category
                    ) as category_counts
                    GROUP BY postcode"""
            )
            pa_table: pa.Table = cursor.fetchall_arrow()
            return pa_table.to_pandas()

    def get_popular_places(self, categories: list[str], zip_code: int) -> pd.DataFrame:
        with self._connection.cursor() as cursor:
            filter_statement = ",".join([f"'{c}'" for c in categories])
            cursor.execute(
                f"""SELECT name, MAX(popularity) as popularity, MAX(provenance_rating) as provenance_rating from(
                        SELECT * FROM(
                            select name, explode(fsq_category_labels[0]) as category, postcode, round(popularity, 2) as popularity, provenance_rating
                            from {self.source_table})
                        WHERE category IN ({filter_statement}) AND postcode == {zip_code})
                        group by 1
                        ORDER BY popularity DESC"""
            )
            pa_table: pa.Table = cursor.fetchall_arrow()
            return pa_table.to_pandas()
