import os
import json
import logging
import boto3
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LogProcessor:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.source_paths = {
            '100k': 'logs/extracted/2024/01/100k',  # Update with actual path
            '30k': 'logs/extracted/2024/01/30k'     # Update with actual path
        }
        self.target_base_path = 'logs/data'

    def fetch_s3_file(self, bucket, key):
        """Fetches an S3 file and returns its lines."""
        try:
            data = self.s3_client.get_object(Bucket=bucket, Key=key)
            lines = data['Body'].read().decode('utf-8').splitlines()
            return lines
        except Exception as e:
            logger.error(f"Error fetching file {key}: {str(e)}")
            return []

    def process_file_lines(self, lines):
        """Processes lines in a file and returns processed records."""
        processed_records = []
        for line in lines:
            try:
                record = self.parse_log_line(line)
                if record:
                    processed_records.append(record)
            except Exception as e:
                logger.warning(f"Error processing line: {line[:50]}...: {str(e)}")
        return processed_records

    def parse_log_line(self, line):
        """Parses a single log line into a structured format."""
        parts = line.strip().split('|')
        if len(parts) != 4:
            return None

        timestamp = datetime.strptime(parts[0], "%Y-%m-%dT%H:%M:%S")
        return {
            'timestamp': timestamp.strftime("%Y-%m-%dT%H:%M:%S"),
            'unique_id': parts[1],
            'event': parts[2],
            'episode_number': parts[3].replace('episode-', ''),
            'year': timestamp.year,
            'month': timestamp.month,
            'day': timestamp.day,
            'time': timestamp.strftime("%H:%M:%S")
        }

    def write_to_s3(self, bucket, folder_name, data):
        """Writes processed data to S3 as a Parquet file."""
        try:
            df = pd.DataFrame(data)
            df['episode_number'] = df['episode_number'].astype(int)
            dates = pd.to_datetime(df['timestamp'])
            year = str(max(dates).year)
            month = str(max(dates).month).zfill(2)

            output_key = f'{self.target_base_path}/{folder_name}/{year}/{month}/{folder_name}_logs.parquet'

            # Save to Parquet with compression
            parquet_buffer = df.to_parquet(index=False, engine='pyarrow', compression='snappy')
            self.s3_client.put_object(
                Bucket=bucket,
                Key=output_key,
                Body=parquet_buffer
            )
            logger.info(f"Successfully wrote {len(df)} records to {output_key}")
            return len(df), year, month
        except Exception as e:
            logger.error(f"Error writing to S3: {str(e)}")
            raise

    def process_logs(self):
        source_bucket = 'raw-data-bronze'
        target_bucket = 'staging-data-silver'

        for log_type, path in self.source_paths.items():
            process_start_time = time.time()
            logger.info(f"\nProcessing {log_type} logs...")

            # Fetch file keys
            paginator = self.s3_client.get_paginator('list_objects_v2')
            file_keys = []
            for page in paginator.paginate(Bucket=source_bucket, Prefix=path):
                file_keys.extend(
                    obj['Key'] for obj in page.get('Contents', []) if obj['Key'].endswith('.txt')
                )

            logger.info(f"Found {len(file_keys)} txt files in {path}")
            if not file_keys:
                logger.warning(f"No {log_type} files found")
                continue

            # Fetch and process files in parallel
            processed_records = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                future_to_key = {
                    executor.submit(self.fetch_s3_file, source_bucket, key): key
                    for key in file_keys
                }
                for future in as_completed(future_to_key):
                    key = future_to_key[future]
                    try:
                        lines = future.result()
                        processed_records.extend(self.process_file_lines(lines))
                    except Exception as e:
                        logger.error(f"Error processing file {key}: {str(e)}")

            logger.info(f"Processed records: {len(processed_records)}")
            if not processed_records:
                continue

            # Write results to S3
            output_count, year, month = self.write_to_s3(target_bucket, log_type, processed_records)
            self.log_metrics(len(file_keys), len(processed_records), output_count, time.time() - process_start_time)

    def log_metrics(self, source_files, processed_records, output_count, duration):
        """Logs performance metrics."""
        logger.info("\nProcessing Metrics:")
        logger.info(f"Source files: {source_files}")
        logger.info(f"Records: {processed_records} processed, {output_count} output")
        logger.info(f"Duration: {round(duration, 2)} seconds")
        if duration > 0:
            logger.info(f"Speed: {round(processed_records / duration, 2)} records/second")


def main():
    processor = LogProcessor()
    try:
        processor.process_logs()
        logger.info("\nProcessing completed successfully!")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()
