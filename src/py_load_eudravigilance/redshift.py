import io
import uuid
from typing import Dict

import boto3
import pandas as pd
import redshift_connector

from .base import BaseLoader


class RedshiftLoader(BaseLoader):
    def __init__(self, config: Dict):
        super().__init__(config)
        self.conn = None
        self.s3_client = boto3.client("s3")

    def connect(self) -> None:
        self.conn = redshift_connector.connect(
            host=self.config["host"],
            database=self.config["database"],
            user=self.config["user"],
            password=self.config["password"],
        )

    def load_dataframe(self, df: pd.DataFrame, table_name: str) -> None:
        s3_staging_path = self.config["s3_staging_path"]
        iam_role_arn = self.config["iam_role_arn"]
        bucket, S3_prefix = s3_staging_path.replace("s3://", "").split("/", 1)
        s3_key = f"{S3_prefix}{uuid.uuid4()}.parquet"

        # Stage DataFrame to S3 as a gzipped Parquet file
        with io.BytesIO() as buffer:
            df.to_parquet(buffer, index=False, compression="gzip")
            buffer.seek(0)
            self.s3_client.upload_fileobj(buffer, bucket, s3_key)

        # Construct and execute a COPY SQL command
        copy_sql = f"""
            COPY {table_name}
            FROM 's3://{bucket}/{s3_key}'
            IAM_ROLE '{iam_role_arn}'
            FORMAT AS PARQUET
            GZIP;
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(copy_sql)
            self.conn.commit()
        finally:
            # Clean up the temporary file from S3
            self.s3_client.delete_object(Bucket=bucket, Key=s3_key)

    def close(self) -> None:
        if self.conn:
            self.conn.close()
