from __future__ import annotations

import json
import warnings
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING

import boto3
from attrs import define, field
from botocore.client import Config as BotocoreClientConfig
from botocore.exceptions import ClientError
from urllib3.exceptions import InsecureRequestWarning

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

@cache
def _get_s3_client(
    credential: S3Credential,
    connect_timeout: int = 10,
    read_timeout: int = 10,
    retries: int = 3,
) -> S3Client:
    """Create a boto3 session for S3."""
    return boto3.client(
        "s3",
        endpoint_url=credential.endpoint_url,
        aws_access_key_id=credential.aws_access_key_id,
        aws_secret_access_key=credential.aws_secret_access_key,
        verify=False,
        config=BotocoreClientConfig(
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            retries={"max_attempts": retries, "mode": "standard"},
        ),
    )


class MissingS3Credential(ValueError):
    def __init__(self, endpoint: str):
        super().__init__(f"No credentials found for endpoint '{endpoint}'")


@define(slots=False, kw_only=True, frozen=True)
class S3Credential:
    endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str


class S3Credentials(dict[str, S3Credential]):
    @classmethod
    def from_file(cls, path: Path) -> S3Credentials:
        if not path.exists():
            raise FileNotFoundError(f"No such file: '{path}'")
        with open(path, "r") as f:
            data = json.load(f)
        if isinstance(data, dict):
            data = [data]
        result = {}
        for item in data:
            endpoint_url = item.pop("endpoint_url", None)
            aws_access_key_id = item.pop("aws_access_key_id", None)
            aws_secret_access_key = item.pop("aws_secret_access_key", None)
            for key in item:
                warnings.warn(f"Unexpected key '{key}' in S3 credential item: {item}")
            try:
                credential = S3Credential(
                    endpoint_url=endpoint_url,
                    aws_access_key_id=aws_access_key_id,
                    aws_secret_access_key=aws_secret_access_key,
                )
                result[endpoint_url] = credential
            except TypeError as exc:
                warnings.warn(f"Failed to create S3Credential for item {item}: {exc}")
                continue
            else:
                result[endpoint_url] = credential
        return cls(**result)

    @classmethod
    def _convert(cls, data: S3Credentials | str | Path | dict[str, dict]) -> S3Credentials:
        if isinstance(data, S3Credentials):
            return data
        if isinstance(data, dict):
            return cls({k: S3Credential(**v) for k, v in data.items()})
        if isinstance(data, str | Path):
            return cls.from_file(Path(data))
        raise ValueError(f"Cannot convert '{type(data).__name__}' to S3Credentials")

    def __getitem__(self, endpoint: str) -> S3Credential:
        try:
            return super().__getitem__(endpoint)
        except KeyError as exc:
            raise MissingS3Credential(endpoint) from exc


@define(slots=False, kw_only=True)
class S3Manager:
    credentials: S3Credentials = field(converter=S3Credentials._convert)

    def fetch(self, endpoint: str, bucket: str, key: str, dst: Path) -> Path:
        if dst.is_dir():
            dst = dst / key.split("/")[-1]
        if dst.exists():
            raise FileExistsError(f"Destination path '{dst}' already exists.")
        s3 = _get_s3_client(self.credentials[endpoint])
        dst.parent.mkdir(parents=True, exist_ok=True)
        with warnings.catch_warnings(action="ignore", category=InsecureRequestWarning):
            s3.download_file(bucket, key, str(dst))
        return dst

    def upload(self, endpoint: str, bucket: str, key: str, src: Path):
        if src.is_dir():
            raise IsADirectoryError(f"Source path '{src}' is a directory, expected a file.")
        if self.exists(endpoint, bucket, key):
            raise FileExistsError(f"Remote file '{key}' already exists in bucket '{bucket}'.")
        s3 = _get_s3_client(self.credentials[endpoint])
        with warnings.catch_warnings(action="ignore", category=InsecureRequestWarning):
            s3.upload_file(Filename=str(src), Bucket=bucket, Key=key, ExtraArgs={"ChecksumAlgorithm": "SHA256"})

    def exists(self, endpoint: str, bucket: str, key: str) -> bool:
        s3 = _get_s3_client(self.credentials[endpoint])
        try:
            with warnings.catch_warnings(action="ignore", category=InsecureRequestWarning):
                s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as exc:
            if exc.response["ResponseMetadata"]["HTTPStatusCode"] == 404:
                return False
            raise
