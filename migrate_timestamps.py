import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from urllib.parse import urlparse

import boto3
import orjson
from boto3.session import Session
from botocore.config import Config

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s"
)


def migrate_timestamp(iso_timestamp_str: str) -> int:
    """Converts an ISO 8601 timestamp string to epoch milliseconds."""
    if iso_timestamp_str.endswith("Z"):
        iso_timestamp_str = iso_timestamp_str[:-1] + "+00:00"
    dt_obj = datetime.fromisoformat(iso_timestamp_str)
    return int(dt_obj.timestamp() * 1000)


def process_record(line: str) -> str:
    """Processes a single line (record) to migrate its timestamp."""
    if not line:
        return ""
    try:
        record = orjson.loads(line)
        if "timestamp" in record and isinstance(record["timestamp"], str):
            record["timestamp"] = migrate_timestamp(record["timestamp"])
        return orjson.dumps(record).decode("utf-8")
    except (orjson.JSONDecodeError, TypeError, ValueError):
        # If a line fails to process, return it as is
        return line


def process_s3_file(session: Session, bucket: str, key: str, max_workers: int):
    """
    Reads an NDJSON file from S3, converts timestamps in parallel, and overwrites the file.
    """
    logging.info(f"Processing s3://{bucket}/{key}")
    try:
        s3_client = session.client("s3")
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response["Body"].read().decode("utf-8")

        lines = content.strip().split("\n")

        # Process records within the file in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            updated_lines = list(executor.map(process_record, lines))

        new_content = "\n".join(updated_lines) + "\n"

        s3_client.put_object(Bucket=bucket, Key=key, Body=new_content.encode("utf-8"))
        logging.info(f"Successfully migrated timestamps in s3://{bucket}/{key}")

    except Exception as e:
        logging.error(f"Failed to process file s3://{bucket}/{key}: {e}", exc_info=True)


def main():
    parser = argparse.ArgumentParser(
        description="Migrate ISO timestamps to epoch milliseconds in NDJSON files on S3."
    )
    parser.add_argument("s3_path", type=str, help="The S3 path (e.g., s3://your-bucket/your-prefix/) to process.")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=5,
        help="Number of parallel workers for processing files and records.",
    )
    args = parser.parse_args()

    parsed_url = urlparse(args.s3_path)
    if parsed_url.scheme != "s3":
        raise ValueError("Please provide a valid S3 path, e.g., s3://your-bucket/your-prefix/")

    bucket_name = parsed_url.netloc
    prefix = parsed_url.path.lstrip("/")

    # Configure boto3 for adaptive retries to respect S3 rate limits
    retry_config = Config(
        retries={
            "max_attempts": 10,
            "mode": "adaptive",
        }
    )
    session = boto3.Session()
    s3_client = session.client("s3", config=retry_config)

    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

    files_to_process = [
        obj["Key"]
        for page in pages
        if "Contents" in page
        for obj in page["Contents"]
        if obj["Key"].endswith(".ndjson")
    ]

    logging.info(f"Found {len(files_to_process)} .ndjson files to process.")

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        futures = [
            executor.submit(process_s3_file, session, bucket_name, key, args.concurrency)
            for key in files_to_process
        ]
        for future in as_completed(futures):
            future.result()  # to raise exceptions if any occurred

    logging.info("Migration process completed.")


if __name__ == "__main__":
    main()
