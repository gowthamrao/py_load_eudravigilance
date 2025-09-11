from typing import Dict

import pandas as pd
from google.cloud import bigquery

from .base import BaseLoader


class BigQueryLoader(BaseLoader):
    def __init__(self, config: Dict):
        super().__init__(config)
        self.client = None

    def connect(self) -> None:
        self.client = bigquery.Client(project=self.config["project"])

    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        dataset_id = self.config["dataset"]
        table_ref = self.client.dataset(dataset_id).table(table_name)
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )
        self.client.load_table_from_dataframe(
            df, table_ref, job_config=job_config
        ).result()

    def close(self) -> None:
        if self.client:
            self.client.close()
