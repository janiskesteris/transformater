import boto3
import botocore
import os
import timeout_decorator
from retry import retry

from bm_product_catalog_etl.config import DATA_DIR, LOGGER


class S3FileDownloader:
    S3_DOWNLOAD_TIMEOUT_SEC = 60

    def __init__(self, bucket, file_path):
        self.bucket = bucket
        self.file_path = file_path

    @retry(
        timeout_decorator.timeout_decorator.TimeoutError,
        tries=5,
        delay=2,
        backoff=2,
        logger=LOGGER,
    )
    @timeout_decorator.timeout(S3_DOWNLOAD_TIMEOUT_SEC)
    def download(self):
        self.__boto_init()
        self.s3_client.download_file(
            self.bucket, self.file_path, self.local_file_path()
        )

    def local_file_path(self):
        return os.path.join(DATA_DIR, os.path.basename(self.file_path))

    def __boto_init(self):
        self.s3_client = boto3.client("s3")
        self.s3_client.meta.events.register(
            "choose-signer.s3.*", botocore.handlers.disable_signing
        )
