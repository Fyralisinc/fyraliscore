from __future__ import annotations

import os

import boto3

from services.ingest.synthetic.validation_runs.moto_lifecycle import moto_s3


def test_moto_lifecycle_creates_raw_and_durable_buckets() -> None:
    with moto_s3(
        bucket="run4-raw",
        blob_bucket="run4-blobs",
    ) as endpoint:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name="us-east-1",
            aws_access_key_id="testing",
            aws_secret_access_key="testing",
        )
        names = {bucket["Name"] for bucket in client.list_buckets().get("Buckets", [])}
        assert names == {"run4-raw", "run4-blobs"}
        assert os.environ["S3_RAW_BUCKET"] == "run4-raw"
        assert os.environ["S3_BLOB_BUCKET"] == "run4-blobs"
