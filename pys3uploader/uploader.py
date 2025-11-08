import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, datetime
from typing import Dict, Iterable, NoReturn

import boto3.resources.factory
import dotenv
from alive_progress import alive_bar
from botocore.config import Config
from botocore.exceptions import ClientError

from pys3uploader.exceptions import BucketNotFound
from pys3uploader.logger import LogHandler, LogLevel, setup_logger
from pys3uploader.metadata import Metadata
from pys3uploader.progress import ProgressPercentage
from pys3uploader.timer import RepeatedTimer
from pys3uploader.utils import (
    RETRY_CONFIG,
    UploadResults,
    convert_seconds,
    convert_to_folder_structure,
    getenv,
    size_converter,
    urljoin,
)


class Uploader:
    """Initiates Uploader object to upload entire directory to S3.

    >>> Uploader

    """

    def __init__(
        self,
        bucket_name: str,
        upload_dir: str,
        s3_prefix: str = None,
        exclude_prefix: str = None,
        skip_dot_files: bool = True,
        overwrite: bool = False,
        file_exclusion: Iterable[str] = None,
        folder_exclusion: Iterable[str] = None,
        metadata_upload_interval: int = None,
        metadata_filename: str = None,
        region_name: str = None,
        profile_name: str = None,
        aws_access_key_id: str = None,
        aws_secret_access_key: str = None,
        retry_config: Config = RETRY_CONFIG,
        logger: logging.Logger = None,
        log_handler: LogHandler = LogHandler.stdout,
        log_level: LogLevel = LogLevel.debug,
        env_file: str = None,
    ):
        """Initiates all the necessary args and creates a boto3 session with retry logic.

        Args:
            bucket_name: Name of the bucket.
            upload_dir: Full path of the directory to be uploaded.
            s3_prefix: Particular bucket prefix within which the upload should happen.
            exclude_prefix: Full directory path to exclude from S3 object prefix.
            skip_dot_files: Boolean flag to skip dot files.
            overwrite: Boolean flag to overwrite files in S3.
            file_exclusion: Sequence of files to exclude during upload.
            folder_exclusion: Sequence of directories to exclude during upload.
            metadata_upload_interval: Interval in seconds to upload metadata file.
            metadata_filename: Metadata filename to upload periodically.
            region_name: Name of the AWS region.
            profile_name: AWS profile name.
            aws_access_key_id: AWS access key ID.
            aws_secret_access_key: AWS secret access key.
            logger: Bring your own logger.
            log_handler: Default log handler, can be ``file`` or ``stdout``.
            log_level: Default log level, can be ``debug``, ``info``, ``warning`` or ``error``.
            env_file: Dotenv file (.env) filepath to load environment variables.

        See Also:
            s3_prefix:
                If provided, ``s3_prefix`` will always be attached to each object.

                If ``s3_prefix`` is set to: ``2025``, then the file path
                ``/home/ubuntu/Desktop/S3Upload/sub/photo.jpg`` will be uploaded as ``2025/S3Upload/sub/photo.jpg``

            exclude_prefix:
                When upload directory is "/home/ubuntu/Desktop/S3Upload", each file will naturally have the full prefix.
                However, this behavior can be avoided by specifying the ``exclude_prefix`` parameter.

                If exclude_prefix is set to: ``/home/ubuntu/Desktop``, then the file path
                ``/home/ubuntu/Desktop/S3Upload/sub-dir/photo.jpg`` will be uploaded as ``S3Upload/sub-dir/photo.jpg``

            env_file:
                Environment variables can be loaded from a .env file.
                The filepath can be set as ``env_file`` during object instantiation or as an environment variable.
                If a filepath is provided, PyS3Uploader loads it directly or searches the root directory for the file.
                If no filepath is provided, PyS3Uploader searches the current directory for a .env file.
        """
        self.logger = logger or setup_logger(handler=LogHandler(log_handler), level=LogLevel(log_level))
        self.env_file = env_file or getenv("ENV_FILE", default=".env")

        # Check for env_file in current working directory
        if os.path.isfile(self.env_file):
            self.logger.debug("Loading env file: %s", self.env_file)
            dotenv.load_dotenv(dotenv_path=self.env_file, override=True)
        # Find the env_file from root
        elif env_file := dotenv.find_dotenv(self.env_file, raise_error_if_not_found=False):
            self.logger.debug("Loading env file: %s", env_file)
            dotenv.load_dotenv(dotenv_path=env_file, override=True)
        else:
            # Scan current working directory for any .env files
            for file in os.listdir():
                if file.endswith(".env"):
                    self.logger.debug("Loading env file: %s", file)
                    dotenv.load_dotenv(dotenv_path=file, override=True)
                    break
            else:
                self.logger.debug("No .env files found to load")

        self.session = boto3.Session(
            profile_name=profile_name or getenv("PROFILE_NAME", "AWS_PROFILE_NAME"),
            region_name=region_name or getenv("AWS_DEFAULT_REGION"),
            aws_access_key_id=aws_access_key_id or getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=aws_secret_access_key or getenv("AWS_SECRET_ACCESS_KEY"),
        )
        self.s3 = self.session.resource(service_name="s3", config=retry_config)

        self.bucket_name = bucket_name
        self.upload_dir = upload_dir
        self.s3_prefix = s3_prefix
        self.exclude_prefix = exclude_prefix
        self.skip_dot_files = skip_dot_files
        self.overwrite = overwrite
        self.file_exclusion = file_exclusion or []
        self.folder_exclusion = folder_exclusion or []

        self.results = UploadResults()
        self.start = time.time()

        # noinspection PyUnresolvedReferences
        self.bucket: boto3.resources.factory.s3.Bucket = None
        # noinspection PyUnresolvedReferences
        self.bucket_objects: boto3.resources.factory.s3.ObjectSummary = []
        self.object_size_map: Dict[str, int] = {}

        self.upload_files: Dict[str, str] = {}
        self.file_size_map: Dict[str, int] = {}

        self.metadata_filename = metadata_filename or getenv("METADATA_FILENAME", default="METADATA.json")
        self.timer = RepeatedTimer(
            function=self.metadata_uploader,
            interval=metadata_upload_interval or int(getenv("METADATA_UPLOAD_INTERVAL", default="300")),
            logger=self.logger,
        )
        self.alive_bar_kwargs = dict(title="Progress", bar="smooth", spinner=None, enrich_print=False)

    def init(self) -> None | NoReturn:
        """Instantiates the bucket instance.

        Raises:
            ValueError: If no bucket name was passed.
            BucketNotFound: If bucket name was not found.
        """
        self.start = time.time()
        if self.exclude_prefix and self.exclude_prefix not in self.upload_dir:
            raise ValueError(
                f"\n\n\tStart folder {self.exclude_prefix!r} is not a part of upload directory {self.upload_dir!r}"
            )
        if not self.upload_dir:
            raise ValueError("\n\n\tCannot proceed without an upload directory.")
        try:
            assert os.path.exists(self.upload_dir)
        except AssertionError:
            raise ValueError(f"\n\n\tPath not found: {self.upload_dir}")
        if not self.bucket_name:
            raise ValueError("\n\n\tCannot proceed without a bucket name.")
        if (buckets := [bucket.name for bucket in self.s3.buckets.all()]) and self.bucket_name not in buckets:
            raise BucketNotFound(f"\n\n\t{self.bucket_name} was not found.\n\tAvailable: {buckets}")
        self.upload_dir = os.path.abspath(self.upload_dir)
        self.load_bucket_state()

    def load_bucket_state(self) -> None:
        """Loads the bucket's current state."""
        # noinspection PyUnresolvedReferences
        self.bucket: boto3.resources.factory.s3.Bucket = self.s3.Bucket(self.bucket_name)
        # noinspection PyUnresolvedReferences
        self.bucket_objects: boto3.resources.factory.s3.ObjectSummary = [obj for obj in self.bucket.objects.all()]
        self.object_size_map = {obj.key: obj.size for obj in self.bucket_objects}

    def load_local_state(self):
        """Loads the local file queue."""
        self.upload_files = self._get_files()
        self.file_size_map = {file: self.filesize(file) for file in self.upload_files}

    def exit(self) -> None:
        """Exits after printing results, and run time."""
        success = len(self.results.success)
        skipped = len(self.results.skipped)
        failed = len(self.results.failed)
        total = success + failed
        self.logger.info(
            "Total number of uploads: %d, skipped: %d, success: %d, failed: %d", total, skipped, success, failed
        )
        # Stop the timer and upload the final state as metadata file
        self.timer.stop()
        self.metadata_uploader()
        self.logger.info("Run time: %s", convert_seconds(time.time() - self.start))

    def filesize(self, filepath: str) -> int:
        """Gets the file size of a given filepath.

        Args:
            filepath: Full path of the file.

        Returns:
            int:
            Returns the file size in bytes.
        """
        try:
            return os.path.getsize(filepath)
        except (OSError, PermissionError) as error:
            self.logger.error(error)
            return 0

    def size_it(self) -> None:
        """Calculates and logs the total size of files in S3 and local."""
        files_in_s3 = len(self.object_size_map)
        files_local = len(self.upload_files)

        total_size_s3 = sum(self.object_size_map.values())
        total_size_local = sum(self.file_size_map.values())

        self.logger.info("Files in S3: [#%d]: %s (%d bytes)", files_in_s3, size_converter(total_size_s3), total_size_s3)
        self.logger.info(
            "Files local: [#%d]: %s (%d bytes)", files_local, size_converter(total_size_local), total_size_local
        )

    def _proceed_to_upload(self, filepath: str, objectpath: str) -> bool:
        """Compares file size if the object already exists in S3.

        Args:
            filepath: Source filepath.
            objectpath: S3 object path.

        Returns:
            bool:
            Returns a boolean flag to indicate upload flag.
        """
        if self.overwrite:
            return True
        file_size = self.filesize(filepath)
        # Indicates that the object path already exists in S3
        if object_size := self.object_size_map.get(objectpath):
            if object_size == file_size:
                self.logger.info(
                    "S3 object %s exists, and size [%d bytes / %s] matches, skipping..",
                    objectpath,
                    object_size,
                    size_converter(object_size),
                )
                self.results.skipped.append(filepath)
                return False
            self.logger.info(
                "S3 object %s exists, but size mismatch. Local: [%d bytes / %s], S3: [%d bytes / %s]",
                objectpath,
                file_size,
                object_size,
                size_converter(object_size),
            )
        else:
            self.logger.debug(
                "S3 object '%s' of size [%d bytes / %s] doesn't exist, uploading..",
                objectpath,
                file_size,
                size_converter(file_size),
            )
        return True

    def _uploader(self, filepath: str, objectpath: str, callback: ProgressPercentage) -> None:
        """Uploads the filepath to the specified S3 bucket.

        Args:
            filepath: Filepath to upload.
            objectpath: Object path ref in S3.
            callback: ProgressPercentage callback to track upload progress.
        """
        if self._proceed_to_upload(filepath, objectpath):
            self.bucket.upload_file(filepath, objectpath, Callback=callback)

    def _get_files(self) -> Dict[str, str]:
        """Get a mapping for all the file path and object paths in upload directory.

        Returns:
            Dict[str, str]:
            Returns a key-value pair of filepath and objectpath.
        """
        files_to_upload = {}
        for __path, __directory, __files in os.walk(self.upload_dir):
            scan_dir = os.path.split(__path)[-1]
            if scan_dir in self.folder_exclusion:
                self.logger.info("Skipping '%s' honoring folder exclusion", scan_dir)
                continue
            for file_ in __files:
                if file_ in self.file_exclusion:
                    self.logger.info("Skipping '%s' honoring file exclusion", file_)
                    continue
                if self.skip_dot_files and file_.startswith("."):
                    self.logger.info("Skipping dot file: %s", file_)
                    continue
                file_path = os.path.join(__path, file_)
                if self.exclude_prefix:
                    relative_path = file_path.replace(self.exclude_prefix, "")
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
                files_to_upload[file_path] = object_path
        return files_to_upload

    def _preflight(self) -> int:
        """Preflight checks and tasks before upload.

        Returns:
            int:
            Returns the total number of files to be uploaded.
        """
        # Verify and initiate bucket state
        self.init()
        # Verify and initiate local state
        self.load_local_state()
        # Make sure there are files to upload
        assert self.upload_files, "\n\n\tNo files found to upload.\n"
        # Log size details
        self.size_it()
        # Start metadata upload timer
        self.timer.start()
        # Return total files to upload
        return len(self.upload_files)

    def run(self) -> None:
        """Initiates object upload in a traditional loop."""
        total_files = self._preflight()

        self.logger.info(
            "%d files from '%s' will be uploaded to '%s' sequentially",
            total_files,
            self.upload_dir,
            self.bucket_name,
        )
        with alive_bar(total_files, **self.alive_bar_kwargs) as overall_bar:
            for filepath, objectpath in self.upload_files.items():
                progress_callback = ProgressPercentage(
                    filename=os.path.basename(filepath), size=self.filesize(filepath), bar=overall_bar
                )
                try:
                    self._uploader(filepath, objectpath, progress_callback)
                    self.results.success.append(filepath)
                except ClientError as error:
                    self.logger.error("Upload failed: %s", error)
                    self.results.failed.append(filepath)
                except KeyboardInterrupt:
                    self.logger.warning("Upload interrupted by user")
                    break
                overall_bar()  # increment overall progress bar
        self.exit()

    def run_in_parallel(self, max_workers: int = 5) -> None:
        """Initiates upload in multi-threading.

        Args:
            max_workers: Number of maximum threads to use.
        """
        total_files = self._preflight()

        self.logger.info(
            "%d files from '%s' will be uploaded to '%s' with maximum concurrency of: %d",
            total_files,
            self.upload_dir,
            self.bucket_name,
            max_workers,
        )
        with alive_bar(total_files, **self.alive_bar_kwargs) as overall_bar:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {}
                for filepath, objectpath in self.upload_files.items():
                    progress_callback = ProgressPercentage(
                        filename=os.path.basename(filepath), size=self.filesize(filepath), bar=overall_bar
                    )
                    future = executor.submit(self._uploader, filepath, objectpath, progress_callback)
                    futures[future] = filepath

                for future in as_completed(futures):
                    filepath = futures[future]
                    try:
                        future.result()
                        self.results.success.append(filepath)
                    except ClientError as error:
                        self.logger.error("Upload failed: %s", error)
                        self.results.failed.append(filepath)
                    overall_bar()  # Increment overall bar after each upload finishes
        self.exit()

    def metadata_uploader(self) -> None:
        """Metadata uploader."""
        self.load_bucket_state()
        success = list(set(self.results.success + self.results.skipped))
        objects_uploaded = len(success)
        size_uploaded = sum(self.filesize(file) for file in success)

        pending_files = set(self.upload_files.keys()) - set(success)
        objects_pending = len(pending_files)
        size_pending = sum(self.filesize(file) for file in pending_files)

        metadata = Metadata(
            timestamp=datetime.now(tz=UTC).strftime("%A %B %d, %Y %H:%M:%S"),
            objects_uploaded=objects_uploaded,
            objects_pending=objects_pending,
            size_uploaded=size_converter(size_uploaded),
            size_pending=size_converter(size_pending),
        )
        self.logger.debug("\n" + json.dumps(metadata.__dict__, indent=2) + "\n")
        self.logger.debug("Uploading metadata to S3")
        filepath = os.path.join(os.getcwd(), self.metadata_filename)
        with open(filepath, "w") as file:
            json.dump(metadata.__dict__, file, indent=2)
            file.flush()
        self.bucket.upload_file(filepath, self.metadata_filename)

    def get_bucket_structure(self) -> str:
        """Gets all the objects in an S3 bucket and forms it into a hierarchical folder like representation.

        Returns:
            str:
            Returns a hierarchical folder like representation of the chosen bucket.
        """
        self.init()
        # Using list and set will yield the same results but using set we can isolate directories from files
        return convert_to_folder_structure(set(obj.key for obj in self.bucket_objects))

    def print_bucket_structure(self) -> None:
        """Prints all the objects in an S3 bucket with a folder like representation."""
        print(self.get_bucket_structure())
