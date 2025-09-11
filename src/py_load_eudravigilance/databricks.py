import uuid
from typing import Dict

import pandas as pd
from databricks import sql

from .base import BaseLoader


class DatabricksLoader(BaseLoader):
    def __init__(self, config: Dict):
        super().__init__(config)
        self.conn = None

    def connect(self) -> None:
        self.conn = sql.connect(
            server_hostname=self.config["server_hostname"],
            http_path=self.config["http_path"],
            access_token=self.config["access_token"],
        )

    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        storage_path = self.config["storage_path"]
        storage_options = self.config.get("storage_options", {})
        file_name = f"{uuid.uuid4()}.parquet"
        full_path = f"{storage_path}/{file_name}"

        # Stage DataFrame to cloud storage as a Parquet file
        df.to_parquet(full_path, **storage_options)

        # Construct and execute a COPY INTO SQL command
        copy_into_sql = f"""
            COPY INTO {table_name}
            FROM '{full_path}'
            FILEFORMAT = PARQUET
            COPY_OPTIONS ('mergeSchema' = 'true')
        """
        with self.conn.cursor() as cursor:
            cursor.execute(copy_into_sql)

    def close(self) -> None:
        if self.conn:
            self.conn.close()
