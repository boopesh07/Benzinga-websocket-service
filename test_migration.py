#!/usr/bin/env python3
"""
Test script for migration functionality using local files.

This script tests the migration logic using the sample legacy files
we have locally before running it on S3.
"""

import asyncio
import json
import logging
import os
import sys
import tempfile
from pathlib import Path

# Add app directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'app'))

from migrate_legacy_data import LegacyRecordTransformer, MigrationStats, S3FileProcessor
from app.bedrock_summarizer import BedrockSummarizer
from app.logging_setup import setup_logging
from app.config import settings


class LocalFileProcessor:
    """Mock S3 processor for testing with local files."""

    def __init__(self, test_dir: str):
        self.test_dir = Path(test_dir)

    def list_ndjson_files(self, prefix: str) -> list[str]:
        """List .ndjson files in test directory."""
        files = []
        for file_path in self.test_dir.glob("*.ndjson"):
            files.append(str(file_path))
        return sorted(files)

    def read_file(self, file_path: str) -> list[dict]:
        """Read and parse a local .ndjson file."""
        records = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue

                try:
                    record = json.loads(line)
                    records.append(record)
                except json.JSONDecodeError as e:
                    print(f"Warning: Invalid JSON in {file_path} at line {line_num}: {e}")
                    continue

        print(f"Read {len(records)} records from {file_path}")
        return records

    def write_file(self, file_path: str, records) -> None:
        """Write transformed records to local file."""
        # Create backup of original file
        backup_path = f"{file_path}.backup"
        if not os.path.exists(backup_path):
            import shutil
            shutil.copy2(file_path, backup_path)
            print(f"Created backup: {backup_path}")

        # Write new content
        ndjson_content = ''.join(record.to_ndjson() for record in records)

        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(ndjson_content)

        print(f"Wrote {len(records)} records to {file_path}")


async def test_migration():
    """Test migration using local files."""
    # Setup logging
    setup_logging(level='DEBUG', log_format='text')

    # Create test directory
    test_dir = Path(__file__).parent / "test_migration"
    test_dir.mkdir(exist_ok=True)

    # Copy test files
    import shutil

    test_files = [
        "ip-172-31-100-156.ec2.internal-seq=000267.ndjson",
        "ip-172-31-100-156.ec2.internal-seq=000004.ndjson"
    ]

    for filename in test_files:
        src_path = Path(__file__).parent / filename
        if src_path.exists():
            dst_path = test_dir / filename
            shutil.copy2(src_path, dst_path)
            print(f"Copied {filename} to test directory")

    # Create local processor
    processor = LocalFileProcessor(str(test_dir))

    # Create summarizer (will fail if no AWS credentials, but that's OK for structure test)
    try:
        summarizer = BedrockSummarizer(
            region_name=settings.aws_region_name,
            model_id=settings.bedrock_model_id,
            max_retries=settings.bedrock_max_retries
        )
        print("Successfully created BedrockSummarizer")
    except Exception as e:
        print(f"Note: Could not create BedrockSummarizer (expected if no AWS credentials): {e}")
        print("Migration will use fallback content instead of AI summaries")
        # Create a mock summarizer for testing
        class MockSummarizer:
            def summarize_article(self, ticker, title, body, teaser=None, max_words=200):
                return f"Mock summary for {ticker}: {title[:50]}..." if body else None
        summarizer = MockSummarizer()

    # Create transformer
    transformer = LegacyRecordTransformer(summarizer, stats)

    # List files
    files = processor.list_ndjson_files("")
    print(f"Found {len(files)} test files")

    # Process each file
    stats = MigrationStats()

    for file_path in files:
        print(f"\nProcessing: {file_path}")
        stats.files_processed += 1

        try:
            # Read legacy records
            legacy_records = processor.read_file(file_path)

            if not legacy_records:
                print(f"Warning: No valid records found in {file_path}")
                continue

            # Transform records
            transformed_records = []
            for record_data in legacy_records:
                stats.records_processed += 1

                # Transform the record
                new_record = transformer.transform_record(record_data)

                if new_record:
                    transformed_records.append(new_record)
                    stats.records_transformed += 1

                    # Track AI vs fallback usage (rough heuristic)
                    if len(new_record.content.split()) <= 50:  # Short content likely fallback
                        stats.html_fallbacks_used += 1
                    else:
                        stats.ai_summaries_generated += 1
                else:
                    stats.records_failed += 1

            # Write transformed records back
            if transformed_records:
                processor.write_file(file_path, transformed_records)
                print(f"Successfully migrated {len(transformed_records)}/{len(legacy_records)} records")

                # Show sample of transformed record
                if transformed_records:
                    sample = transformed_records[0]
                    print(f"Sample transformed record: ticker={sample.ticker}, action={sample.action}")
                    print(f"Content length: {len(sample.content)} chars")
                    print(f"Content preview: {sample.content[:100]}...")
            else:
                print(f"Warning: No records successfully transformed in {file_path}")

        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            stats.errors.append(f"File {file_path}: {str(e)}")

    # Show results
    print("
=== MIGRATION RESULTS ===")
    print(f"Files processed: {stats.files_processed}")
    print(f"Records processed: {stats.records_processed}")
    print(f"Records transformed: {stats.records_transformed}")
    print(f"Records failed: {stats.records_failed}")
    print(f"AI summaries generated: {stats.ai_summaries_generated}")
    print(f"HTML fallbacks used: {stats.html_fallbacks_used}")

    if stats.errors:
        print(f"Errors: {len(stats.errors)}")
        for error in stats.errors:
            print(f"  - {error}")

    # Show a sample of the transformed output
    if transformed_records:
        print("
=== SAMPLE TRANSFORMED RECORD ===")
        sample = transformed_records[0]
        print(json.dumps(sample.model_dump(), indent=2, default=str))

    return stats


if __name__ == '__main__':
    asyncio.run(test_migration())
