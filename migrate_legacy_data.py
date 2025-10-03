#!/usr/bin/env python3
"""
Legacy Data Migration Script

Migrates legacy .ndjson files from S3 to the new format by:
1. Reading legacy .ndjson files from S3
2. Extracting raw HTML content from 'body' field
3. Generating Claude AI summaries using existing BedrockSummarizer
4. Transforming records to new format (adding 'action', replacing 'body' with 'content')
5. Writing transformed records back to S3

Usage:
    python migrate_legacy_data.py --s3-path s3://bucket/path/to/legacy/files/
"""

import argparse
import asyncio
import io
import json
import logging
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field

import boto3
import orjson
from pydantic import ValidationError

# Add app directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from app.config import settings
from app.logging_setup import setup_logging
from app.bedrock_summarizer import BedrockSummarizer
from app.text_utils import strip_html_tags
from app.models import OutputRecord


@dataclass
class MigrationStats:
    """Track migration statistics and progress."""
    files_processed: int = 0
    records_processed: int = 0
    records_transformed: int = 0
    records_skipped: int = 0
    records_failed: int = 0
    ai_summaries_generated: int = 0
    html_fallbacks_used: int = 0
    start_time: Optional[datetime] = None
    errors: List[str] = field(default_factory=list)
    api_calls_made: int = 0
    batch_count: int = 0
    file_stats: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def start(self) -> None:
        """Start timing the migration."""
        self.start_time = datetime.now()

    def get_duration(self) -> float:
        """Get migration duration in seconds."""
        if self.start_time:
            return (datetime.now() - self.start_time).total_seconds()
        return 0.0

    def get_api_calls_per_minute(self) -> float:
        """Get API calls per minute rate."""
        duration_minutes = self.get_duration() / 60.0
        if duration_minutes > 0:
            return self.api_calls_made / duration_minutes
        return 0.0

    def log_summary(self) -> None:
        """Log migration summary."""
        duration = self.get_duration()
        api_rate = self.get_api_calls_per_minute()
        # Calculate file statistics
        total_files_with_stats = len(self.file_stats)
        corrupted_files = sum(1 for stats in self.file_stats.values() if stats.get('total_lines', 0) > 0 and stats.get('valid_records', 0) / stats['total_lines'] < 0.5)

        logging.info(
            "Migration completed in %.2f seconds (%.2f minutes). "
            "Files: %d (corrupted: %d), Records: %d (transformed: %d, skipped: %d, failed: %d), "
            "AI summaries: %d, API calls: %d (%.1f/min), Batches: %d",
            duration, duration/60, self.files_processed, corrupted_files, self.records_processed,
            self.records_transformed, self.records_skipped, self.records_failed,
            self.ai_summaries_generated, self.api_calls_made, api_rate, self.batch_count
        )



class LegacyRecordTransformer:
    """Transforms legacy records to new format."""

    def __init__(self, summarizer: BedrockSummarizer, stats: MigrationStats):
        self.summarizer = summarizer
        self.stats = stats
        self.logger = logging.getLogger(__name__)

    def transform_record(self, legacy_data: Dict[str, Any]) -> Optional[OutputRecord]:
        """
        Transform a legacy record to new format.

        Args:
            legacy_data: Raw legacy record data

        Returns:
            Transformed OutputRecord or None if transformation fails
        """
        try:
            # Validate required fields
            if not self._validate_legacy_record(legacy_data):
                return None

            # Extract and clean HTML content
            body_html = legacy_data.get('body', '')
            teaser = legacy_data.get('teaser', '')

            # Clean HTML content
            body_clean = strip_html_tags(body_html) if body_html else ''
            teaser_clean = strip_html_tags(teaser) if teaser else ''

            # Check if this record already contains fallback content (from previous migration)
            existing_content = legacy_data.get('content', '')
            if existing_content and "I apologize, but I don't see any article" in existing_content:
                self.logger.debug(
                    "Record news_id=%s already contains fallback content, skipping",
                    legacy_data.get('news_id')
                )
                return None  # Skip records that already have fallback content

            # Get the raw content (HTML) - pass it directly to LLM for processing
            raw_content = legacy_data.get('body', '')

            if not raw_content.strip():
                self.logger.debug(
                    "Empty content for news_id=%s, skipping record",
                    legacy_data.get('news_id')
                )
                return None  # Skip records with no content

            # For records that might have malformed JSON but contain content,
            # try to extract what we can and let the LLM handle the rest

            # Generate AI summary
            ticker = legacy_data.get('ticker', '')
            title = legacy_data.get('title', '')

            summary = self.summarizer.summarize_html_content(
                ticker=ticker,
                title=title,
                html_content=raw_content,
                max_words=settings.summary_max_words
            )

            # Track API call
            self.stats.api_calls_made += 1

            if not summary:
                self.logger.warning(
                    "AI summarization failed for news_id=%s, skipping record",
                    legacy_data.get('news_id')
                )
                return None  # Skip records where AI summarization fails

            # Build new record
            new_record = OutputRecord(
                timestamp=legacy_data.get('timestamp'),
                ticker=legacy_data.get('ticker', ''),
                news_id=legacy_data.get('news_id'),
                action="Created",  # Default action for legacy records
                title=legacy_data.get('title'),
                content=summary,
                authors=legacy_data.get('authors', []),
                url=legacy_data.get('url'),
                channels=legacy_data.get('channels', []),
                created_at=legacy_data.get('created_at'),
                updated_at=legacy_data.get('updated_at')
            )

            return new_record

        except Exception as e:
            self.logger.error(
                "Failed to transform record news_id=%s: %s",
                legacy_data.get('news_id'), str(e)
            )
            return None

    def _validate_legacy_record(self, data: Dict[str, Any]) -> bool:
        """Validate that legacy record has required fields and content."""
        required_fields = ['news_id', 'ticker']
        missing_fields = [field for field in required_fields if field not in data]

        if missing_fields:
            self.logger.warning(
                "Legacy record missing required fields %s: %s",
                missing_fields, data.get('news_id')
            )
            return False

        # Also check if there's any content to process
        body_content = data.get('body', '')
        if not body_content or not body_content.strip():
            self.logger.debug(
                "Legacy record has no content to process: %s",
                data.get('news_id')
            )
            return False

        return True


class S3FileProcessor:
    """Handles S3 file operations for migration."""

    def __init__(self, bucket: str, region_name: Optional[str] = None):
        self.bucket = bucket
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.logger = logging.getLogger(__name__)

    def list_ndjson_files(self, prefix: str) -> List[str]:
        """List all .ndjson files in the given S3 prefix."""
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            files = []

            for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key.endswith('.ndjson'):
                        files.append(key)

            self.logger.info("Found %d .ndjson files in %s", len(files), prefix)
            return sorted(files)

        except Exception as e:
            self.logger.error("Failed to list files in %s: %s", prefix, str(e))
            raise

    def read_file(self, key: str) -> List[Dict[str, Any]]:
        """Read and parse a .ndjson file from S3 with robust error handling."""
        try:
            response = self.s3_client.get_object(Bucket=self.bucket, Key=key)
            content = response['Body'].read().decode('utf-8')

            records = []
            total_lines = 0

            for line_num, line in enumerate(content.strip().split('\n'), 1):
                total_lines += 1
                line = line.strip()
                if not line:
                    continue

                try:
                    record = orjson.loads(line)
                    records.append(record)
                except json.JSONDecodeError as e:
                    # For severely corrupted files, don't spam the logs
                    if total_lines < 50 or line_num % 100 == 0:  # Log first 50 and every 100th
                        self.logger.warning(
                            "Invalid JSON in file %s at line %d: %s",
                            key, line_num, str(e)
                        )
                    # Skip malformed lines but continue processing
                    continue
                except Exception as e:
                    # Handle any other parsing errors
                    self.logger.warning(
                        "Error parsing line %d in file %s: %s",
                        line_num, key, str(e)
                    )
                    continue

            success_rate = len(records) / total_lines if total_lines > 0 else 0
            self.logger.info(
                "Read %d valid records from %s (%d total lines, %.1f%% success rate)",
                len(records), key, total_lines, success_rate * 100
            )

            return records

        except Exception as e:
            self.logger.error("Failed to read file %s: %s", key, str(e))
            # Don't raise - continue processing other files
            return []


    def write_file(self, key: str, records: List[OutputRecord]) -> None:
        """Write transformed records back to S3."""
        try:
            # Convert records to NDJSON format
            ndjson_content = ''.join(record.to_ndjson() for record in records)

            # Write to S3
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=ndjson_content.encode('utf-8'),
                ContentType='application/x-ndjson'
            )

            self.logger.debug("Wrote %d records to %s", len(records), key)

        except Exception as e:
            self.logger.error("Failed to write file %s: %s", key, str(e))
            raise


class LegacyDataMigrator:
    """Main migration orchestrator with rate limiting and batching."""

    def __init__(self, s3_path: str, batch_size: int = 10, batch_delay_seconds: float = 5.0):
        # Parse S3 path (format: s3://bucket/path/)
        if not s3_path.startswith('s3://'):
            raise ValueError("S3 path must start with 's3://'")

        path_parts = s3_path[5:].split('/', 1)
        self.bucket = path_parts[0]
        self.prefix = path_parts[1].rstrip('/') + '/' if len(path_parts) > 1 else ''

        self.stats = MigrationStats()
        self.s3_processor = S3FileProcessor(self.bucket, settings.aws_region_name)
        self.transformer = LegacyRecordTransformer(self._create_summarizer(), self.stats)
        self.logger = logging.getLogger(__name__)
        self.s3_path = s3_path  # Store for logging

        # Rate limiting configuration
        self.batch_size = batch_size
        self.batch_delay_seconds = batch_delay_seconds
        self.target_api_rate = 120  # 70% of ~170 requests/minute for Claude 3.5 Sonnet

        # Track file statistics for debugging
        self.file_stats = {}

    def _create_summarizer(self) -> BedrockSummarizer:
        """Create BedrockSummarizer instance using existing configuration."""
        return BedrockSummarizer(
            region_name=settings.aws_region_name,
            model_id=settings.bedrock_model_id,
            max_retries=settings.bedrock_max_retries
        )

    async def migrate_files(self) -> MigrationStats:
        """Migrate all legacy files in the S3 path with rate limiting and batching."""
        self.stats.start()

        try:
            # List all .ndjson files
            files = self.s3_processor.list_ndjson_files(self.prefix)

            if not files:
                self.logger.warning("No .ndjson files found in %s", self.s3_path)
                return self.stats

            total_files = len(files)
            self.logger.info(
                "Starting batched migration: %d files in batches of %d (delay: %.1fs between batches)",
                total_files, self.batch_size, self.batch_delay_seconds
            )

            # Process files in batches
            for i in range(0, total_files, self.batch_size):
                batch_files = files[i:i + self.batch_size]
                self.stats.batch_count += 1

                batch_start = datetime.now()
                self.logger.info(
                    "Processing batch %d/%d (%d files)",
                    self.stats.batch_count,
                    (total_files + self.batch_size - 1) // self.batch_size,
                    len(batch_files)
                )

                # Process files in this batch
                for file_key in batch_files:
                    await self._migrate_file(file_key)

                # Calculate API rate and adjust delay if needed
                current_rate = self.stats.get_api_calls_per_minute()
                batch_duration = (datetime.now() - batch_start).total_seconds()

                if current_rate > self.target_api_rate and batch_duration < 60:
                    # We're exceeding target rate, increase delay
                    additional_delay = min(10.0, (current_rate - self.target_api_rate) / 10)
                    actual_delay = self.batch_delay_seconds + additional_delay
                    self.logger.info(
                        "API rate %.1f/min exceeds target %.1f/min, increasing delay to %.1fs",
                        current_rate, self.target_api_rate, actual_delay
                    )
                else:
                    actual_delay = self.batch_delay_seconds

                # Wait between batches (except for the last batch)
                if i + self.batch_size < total_files:
                    self.logger.info("Waiting %.1f seconds before next batch...", actual_delay)
                    await asyncio.sleep(actual_delay)

                # Log progress
                progress_pct = ((i + len(batch_files)) / total_files) * 100
                remaining_files = total_files - (i + len(batch_files))
                self.logger.info(
                    "Batch %d completed. Progress: %.1f%% (%d files remaining)",
                    self.stats.batch_count, progress_pct, remaining_files
                )

            self.stats.log_summary()
            return self.stats

        except Exception as e:
            self.logger.error("Migration failed: %s", str(e))
            self.stats.errors.append(str(e))
            raise


    async def _migrate_file(self, file_key: str) -> None:
        """Migrate a single file."""
        self.logger.info("Processing file: %s", file_key)
        self.stats.files_processed += 1

        try:
            # Read legacy records
            legacy_records = self.s3_processor.read_file(file_key)

            if not legacy_records:
                self.logger.warning("No valid records found in %s", file_key)
                return

            self.logger.info("Starting transformation of %d records from %s", len(legacy_records), file_key)

            # Transform records
            transformed_records = []
            for record_data in legacy_records:
                self.stats.records_processed += 1

                # Transform the record
                new_record = self.transformer.transform_record(record_data)

                if new_record:
                    transformed_records.append(new_record)
                    self.stats.records_transformed += 1

                    # Track AI summaries vs other content
                    if len(new_record.content.split()) <= 100:  # Very short content might indicate issues
                        self.stats.html_fallbacks_used += 1
                    else:
                        self.stats.ai_summaries_generated += 1
                else:
                    self.stats.records_failed += 1

            # Write transformed records back to S3 (overwrite original)
            if transformed_records:
                self.s3_processor.write_file(file_key, transformed_records)
                self.logger.info(
                    "Successfully migrated %d/%d records in %s",
                    len(transformed_records), len(legacy_records), file_key
                )
            else:
                # All records were skipped (no content or fallback content), write empty file
                self.logger.info("All records skipped (no valid content), writing empty file: %s", file_key)
                self.s3_processor.write_file(file_key, [])
                self.logger.info("Wrote empty file for %s (all records had no valid content)", file_key)

        except Exception as e:
            self.logger.error("Failed to migrate file %s: %s", file_key, str(e))
            self.stats.errors.append(f"File {file_key}: {str(e)}")
            # Don't re-raise - continue with next file


async def main():
    """Main entry point for the migration script."""
    parser = argparse.ArgumentParser(description='Migrate legacy .ndjson files to new format with rate limiting')
    parser.add_argument(
        '--s3-path',
        required=True,
        help='S3 path containing legacy .ndjson files (format: s3://bucket/path/)'
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=10,
        help='Number of files to process per batch (default: 10)'
    )
    parser.add_argument(
        '--batch-delay',
        type=float,
        default=5.0,
        help='Delay in seconds between batches (default: 5.0)'
    )
    parser.add_argument(
        '--log-level',
        default='INFO',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        help='Logging level'
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(level=args.log_level, log_format=settings.log_format)

    # Validate S3 path format
    if not args.s3_path.startswith('s3://'):
        print("Error: S3 path must start with 's3://'", file=sys.stderr)
        sys.exit(1)

    print("🚀 Migration Configuration:")
    print(f"   S3 Path: {args.s3_path}")
    print(f"   Batch Size: {args.batch_size} files")
    print(f"   Batch Delay: {args.batch_delay}s")
    print(f"   Target API Rate: 120 calls/min (70% of Bedrock limit)")
    print()

    # Create migrator and run migration
    migrator = LegacyDataMigrator(
        args.s3_path,
        batch_size=args.batch_size,
        batch_delay_seconds=args.batch_delay
    )

    try:
        stats = await migrator.migrate_files()

        # Exit with error code if there were failures
        if stats.records_failed > 0 or stats.errors:
            print(f"\n❌ Migration completed with {stats.records_failed} failed records and {len(stats.errors)} errors")
            sys.exit(1)
        else:
            print(f"\n✅ Migration completed successfully! Processed {stats.records_transformed} records.")

    except KeyboardInterrupt:
        print("\n⏹️  Migration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"Migration failed: {e}", file=sys.stderr)
        sys.exit(1)



if __name__ == '__main__':
    asyncio.run(main())
