"""Utility functions for uploading files to Amazon S3."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import boto3

from .config import S3Info

LOGGER = logging.getLogger(__name__)


def upload_to_s3(path: Path, s3_info: S3Info, object_name: Optional[str] = None) -> bool:
    """Upload a file at *path* to S3.

    Parameters
    ----------
    path:
        The local file to upload.
    s3_info:
        S3 credentials and destination bucket information.
    object_name:
        The key to use when storing the file. Defaults to the local file name.
    """

    if object_name is None:
        object_name = path.name

    LOGGER.info("Uploading %s to s3://%s/%s", path, s3_info.bucket_name, object_name)

    s3_client = boto3.client(
        "s3",
        aws_access_key_id=s3_info.access_key_id,
        aws_secret_access_key=s3_info.secret_access_key,
    )

    try:
        s3_client.upload_file(str(path), s3_info.bucket_name, object_name)
    except Exception:  # pragma: no cover - network error
        LOGGER.exception("Failed to upload %s", path)
        return False

    LOGGER.info("Upload succeeded")
    return True


__all__ = ["upload_to_s3"]
