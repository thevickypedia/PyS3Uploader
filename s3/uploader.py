import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import boto3.resources.factory
from botocore.config import Config
from tqdm import tqdm

from s3.exceptions import BucketNotFound
from s3.logger import default_logger
from s3.utils import get_object_path, getenv


# noinspection PyUnresolvedReference
class Uploader:
    """Initiates Uploader object to upload entire directory to S3.

    >>> Uploader

    """

    RETRY_CONFIG: Config = Config(
        retries={
            "max_attempts": 10,
            "mode": "standard"
        }
    )

    def __init__(
            self,
            bucket_name: str,
            upload_dir: str,
            prefix_dir: str = None,
            region_name: str = None,
            profile_name: str = None,
            aws_access_key_id: str = None,
            aws_secret_access_key: str = None,
            logger: logging.Logger = None,
    ):
        """Initiates all the necessary args and creates a boto3 session with retry logic.

        Args:
            bucket_name: Name of the bucket.
            upload_dir: Name of the directory to be uploaded.
            prefix_dir: Start folder name from upload_dir.
            region_name: Name of the AWS region.
            profile_name: AWS profile name.
            aws_access_key_id: AWS access key ID.
            aws_secret_access_key: AWS secret access key.
            logger: Bring your own logger.
        """
        self.session = boto3.Session(
            profile_name=profile_name or os.environ.get("PROFILE_NAME"),
            region_name=region_name or os.environ.get("AWS_DEFAULT_REGION"),
            aws_access_key_id=aws_access_key_id or os.environ.get("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=aws_secret_access_key or os.environ.get("AWS_SECRET_ACCESS_KEY")
        )
        self.s3 = self.session.resource(service_name="s3", config=self.RETRY_CONFIG)
        self.logger = logger or default_logger()
        self.upload_dir = upload_dir or getenv("UPLOAD_DIR", "SOURCE")
        self.prefix_dir = prefix_dir
        self.bucket_name = bucket_name
        self.bucket: boto3.resources.factory.s3.Bucket = None

    def init(self) -> None:
        """Instantiates the bucket instance.

        Raises:
            ValueError: If no bucket name was passed.
            BucketNotFound: If bucket name was not found.
        """
        if self.prefix_dir and self.prefix_dir not in self.upload_dir.split(os.sep):
            raise ValueError(
                f"\n\n\tPrefix folder name {self.prefix_dir!r} is not a part of upload directory {self.upload_dir!r}"
            )
        if not self.upload_dir:
            raise ValueError(
                f"\n\n\tCannot proceed without a upload directory."
            )
        try:
            assert os.path.exists(self.upload_dir)
        except AssertionError:
            raise ValueError(f"\n\n\tPath not found: {self.upload_dir}")
        buckets = [bucket.name for bucket in self.s3.buckets.all()]
        if not self.bucket_name:
            raise ValueError(
                f"\n\n\tCannot proceed without a bucket name.\n\tAvailable: {buckets}"
            )
        _account_id, _alias = self.session.resource(service_name="iam").CurrentUser().arn.split("/")
        if self.bucket_name not in buckets:
            raise BucketNotFound(
                f"\n\n\t{self.bucket_name} was not found in {_alias} account.\n\tAvailable: {buckets}"
            )
        self.upload_dir = os.path.abspath(self.upload_dir)
        self.logger.info("Bucket objects from '%s' will be uploaded to '%s'",
                         self.upload_dir, self.bucket_name)
        self.bucket: boto3.resources.factory.s3.Bucket = self.s3.Bucket(self.bucket_name)

    def uploader(self, filepath: str, objectpath: str = None) -> None:
        self.bucket.upload_file(filepath, objectpath or filepath)

    def get_files(self):
        files_to_upload = {}
        for __path, __directory, __files in os.walk(self.upload_dir):
            for file_ in __files:
                file_path = os.path.join(__path, file_)
                try:
                    object_path = get_object_path(file_path, self.prefix_dir)
                except ValueError as error:
                    self.logger.error(error)
                    continue
                files_to_upload[object_path] = file_path
        return files_to_upload

    def run(self) -> None:
        """Initiates bucket download in a traditional loop."""
        self.init()
        keys = self.get_files()
        self.logger.debug(keys)
        self.logger.info("Initiating upload process.")
        for objectpath, filepath in tqdm(
                keys.items(), total=len(keys), unit="file", leave=True,
                desc=f"Uploading files from {self.upload_dir}"
        ):
            self.uploader(filepath=filepath, objectpath=objectpath)

    def run_in_parallel(self, max_workers: int = 5) -> None:
        """Initiates upload in multi-threading.

        Args:
            max_workers: Number of maximum threads to use.
        """
        self.init()
        self.logger.info(f"Number of threads: {max_workers}")
        keys = self.get_files()
        self.logger.info("Initiating upload process.")
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # TODO: Fix this with future check
            list(tqdm(iterable=executor.map(self.uploader, keys),
                      total=len(keys), desc=f"Uploading files from {self.bucket_name}",
                      unit="files", leave=True))
        self.logger.info(f"Run Time: {round(float(time.perf_counter()), 2)}s")
