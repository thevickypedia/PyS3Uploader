import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict

import boto3.resources.factory
from botocore.config import Config
from botocore.exceptions import ClientError
from tqdm import tqdm

from s3.exceptions import BucketNotFound
from s3.logger import default_logger
from s3.utils import UploadResults, convert_to_folder_structure, getenv, urljoin


class Uploader:
    """Initiates Uploader object to upload entire directory to S3.

    >>> Uploader

    """

    RETRY_CONFIG: Config = Config(retries={"max_attempts": 10, "mode": "standard"})

    def __init__(
        self,
        bucket_name: str,
        upload_dir: str,
        s3_prefix: str = None,
        exclude_path: str = None,
        region_name: str = None,
        profile_name: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        logger: logging.Logger = None,
    ):
        """Initiates all the necessary args and creates a boto3 session with retry logic.

        Args:
            bucket_name: Name of the bucket.
            upload_dir: Full path of the directory to be uploaded.
            s3_prefix: Particular bucket prefix within which the upload should happen.
            exclude_path: Full directory path to exclude from S3 object prefix.
            region_name: Name of the AWS region.
            profile_name: AWS profile name.
            aws_access_key_id: AWS access key ID.
            aws_secret_access_key: AWS secret access key.
            logger: Bring your own logger.

        See Also:
            exclude_path:
                When upload directory is "/home/ubuntu/Desktop/S3Upload", each file will naturally have the full prefix.
                However, this behavior can be avoided by specifying the ``exclude_path`` parameter.

                If exclude_path is set to: ``/home/ubuntu/Desktop``, then the file path
                ``/home/ubuntu/Desktop/S3Upload/sub-dir/photo.jpg`` will be uploaded as ``S3Upload/sub-dir/photo.jpg``

            s3_prefix:
                If provided, ``s3_prefix`` will always be attached to each object.

                If ``s3_prefix`` is set to: ``2025``, then the file path
                ``/home/ubuntu/Desktop/S3Upload/sub/photo.jpg`` will be uploaded as ``2025/S3Upload/sub/photo.jpg``
        """
        self.session = boto3.Session(
            profile_name=profile_name or getenv("PROFILE_NAME"),
            region_name=region_name or getenv("AWS_DEFAULT_REGION"),
            aws_access_key_id=aws_access_key_id or getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=aws_secret_access_key or getenv("AWS_SECRET_ACCESS_KEY"),
        )
        self.s3 = self.session.resource(service_name="s3", config=self.RETRY_CONFIG)
        self.logger = logger or default_logger()
        self.upload_dir = upload_dir or getenv("UPLOAD_DIR", "UPLOAD_SOURCE")
        self.s3_prefix = s3_prefix
        self.exclude_path = exclude_path
        self.bucket_name = bucket_name
        # noinspection PyUnresolvedReferences
        self.bucket: boto3.resources.factory.s3.Bucket = None
        self.results = UploadResults()
        self.start = time.time()

    def init(self) -> None:
        """Instantiates the bucket instance.

        Raises:
            ValueError: If no bucket name was passed.
            BucketNotFound: If bucket name was not found.
        """
        self.start = time.time()
        if self.exclude_path and self.exclude_path not in self.upload_dir:
            raise ValueError(
                f"\n\n\tStart folder {self.exclude_path!r} is not a part of upload directory {self.upload_dir!r}"
            )
        if not self.upload_dir:
            raise ValueError("\n\n\tCannot proceed without an upload directory.")
        try:
            assert os.path.exists(self.upload_dir)
        except AssertionError:
            raise ValueError(f"\n\n\tPath not found: {self.upload_dir}")
        buckets = [bucket.name for bucket in self.s3.buckets.all()]
        if not self.bucket_name:
            raise ValueError(f"\n\n\tCannot proceed without a bucket name.\n\tAvailable: {buckets}")
        _account_id, _alias = self.session.resource(service_name="iam").CurrentUser().arn.split("/")
        if self.bucket_name not in buckets:
            raise BucketNotFound(f"\n\n\t{self.bucket_name} was not found in {_alias} account.\n\tAvailable: {buckets}")
        self.upload_dir = os.path.abspath(self.upload_dir)
        # noinspection PyUnresolvedReferences
        self.bucket: boto3.resources.factory.s3.Bucket = self.s3.Bucket(self.bucket_name)

    def exit(self) -> None:
        """Exits after printing results, and run time."""
        total = self.results.success + self.results.failed
        self.logger.info(
            "Total number of uploads: %d, success: %d, failed: %d", total, self.results.success, self.results.failed
        )
        self.logger.info("Run Time: %.2fs", time.time() - self.start)

    def _uploader(self, objectpath: str, filepath: str) -> None:
        """Uploads the filepath to the specified S3 bucket.

        Args:
            objectpath: Object path ref in S3.
            filepath: Filepath to upload.
        """
        self.bucket.upload_file(filepath, objectpath)

    def _get_files(self) -> Dict[str, str]:
        """Get a mapping for all the file path and object paths in upload directory.

        Returns:
            Dict[str, str]:
            Returns a dictionary object path and filepath.
        """
        files_to_upload = {}
        for __path, __directory, __files in os.walk(self.upload_dir):
            for file_ in __files:
                file_path = os.path.join(__path, file_)
                if self.exclude_path:
                    relative_path = file_path.replace(self.exclude_path, "")
                else:
                    relative_path = file_path
                # Lists in python are ordered, so s3 prefix will get loaded first when provided
                url_parts = []
                if self.s3_prefix:
                    url_parts.extend(
                        self.s3_prefix.split(os.sep) if os.sep in self.s3_prefix else self.s3_prefix.split("/")
                    )
                # Add rest of the file path to parts before normalizing as an S3 object URL
                url_parts.extend(relative_path.split(os.sep))
                # Remove falsy values using filter - "None", "bool", "len" or "lambda item: item"
                object_path = urljoin(*filter(None, url_parts))
                files_to_upload[object_path] = file_path
        return files_to_upload

    def run(self) -> None:
        """Initiates object upload in a traditional loop."""
        self.init()
        keys = self._get_files()
        self.logger.debug(keys)
        self.logger.info("%d files from '%s' will be uploaded to '%s'", len(keys), self.upload_dir, self.bucket_name)
        self.logger.info("Initiating upload process.")
        for objectpath, filepath in tqdm(
            keys.items(), total=len(keys), unit="file", leave=True, desc=f"Uploading files from {self.upload_dir}"
        ):
            try:
                self._uploader(objectpath=objectpath, filepath=filepath)
                self.results.success += 1
            except ClientError as error:
                self.logger.error(error)
                self.results.failed += 1
        self.exit()

    def run_in_parallel(self, max_workers: int = 5) -> None:
        """Initiates upload in multi-threading.

        Args:
            max_workers: Number of maximum threads to use.
        """
        self.init()
        keys = self._get_files()
        self.logger.debug(keys)
        self.logger.info(
            "%d files from '%s' will be uploaded to '%s' with maximum concurrency of: %d",
            len(keys),
            self.upload_dir,
            self.bucket_name,
            max_workers,
        )
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self._uploader, *kv) for kv in keys.items()]
            for future in tqdm(
                iterable=as_completed(futures),
                total=len(futures),
                desc=f"Uploading files to {self.bucket_name}",
                unit="files",
                leave=True,
            ):
                try:
                    future.result()
                    self.results.success += 1
                except ClientError as error:
                    self.logger.error(f"Upload failed: {error}")
                    self.results.failed += 1
        self.exit()

    def get_bucket_structure(self) -> str:
        """Gets all the objects in an S3 bucket and forms it into a hierarchical folder like representation.

        Returns:
            str:
            Returns a hierarchical folder like representation of the chosen bucket.
        """
        self.init()
        # Using list and set will yield the same results but using set we can isolate directories from files
        return convert_to_folder_structure(set([obj.key for obj in self.bucket.objects.all()]))

    def print_bucket_structure(self) -> None:
        """Prints all the objects in an S3 bucket with a folder like representation."""
        print(self.get_bucket_structure())
