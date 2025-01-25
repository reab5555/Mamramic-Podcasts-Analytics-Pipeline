import logging
import boto3
import zipfile
import io
from pathlib import Path
from datetime import datetime
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ParallelChunkExtractor:
    """
    Processes each ZIP file *sequentially*, but the internal contents
    of the ZIP are extracted in parallel (chunks) with up to `chunk_workers` threads.
    """

    def __init__(self, chunk_size=10000, chunk_workers=8):
        """
        :param chunk_size: Number of files from the ZIP to process in each chunk.
        :param chunk_workers: Number of threads to use for parallel chunk extraction.
        """
        # Use default boto3 client (auto-refresh credentials if on EC2/Cloud9 with an IAM role).
        self.s3_client = boto3.client("s3")

        self.chunk_size = chunk_size
        self.chunk_workers = chunk_workers

        # Separate local directories for 100k vs. 30k
        # Make sure you actually have these folders and put the right ZIPs in them!
        self.local_zip_paths = {
            "100k": "/opt/airflow/dags/data/100k",  # ONLY put 100k zip files here
            "30k": "/opt/airflow/dags/data/30k",    # ONLY put 30k zip files here
        }

        # S3 archive paths for each log type
        self.archive_paths = {
            "100k": "logs/archives/2024/01/100k",
            "30k": "logs/archives/2024/01/30k",
        }

        # Hard-coded extraction base path to year=2024, month=01
        self.extracted_base_path = "logs/extracted"
        self.extract_year = "2024"
        self.extract_month = "01"

    def upload_local_zips(self, bucket_name):
        """
        Upload local .zip files to S3 under the given bucket/archive path.
        Each log_type has its own local directory and S3 prefix.
        """
        for log_type, local_path in self.local_zip_paths.items():
            archive_path = self.archive_paths[log_type]
            logger.info(f"Uploading local .zip files from {local_path} "
                        f"to s3://{bucket_name}/{archive_path}/...")

            # Find all .zip files in the appropriate local path
            zip_files = list(Path(local_path).glob("*.zip"))
            if not zip_files:
                logger.warning(f"No .zip files found in {local_path}")
                continue

            with tqdm(total=len(zip_files),
                      desc=f"Uploading {log_type} .zip files",
                      unit="file") as pbar:
                for zip_file in zip_files:
                    s3_key = f"{archive_path}/{zip_file.name}"
                    try:
                        self.s3_client.upload_file(str(zip_file), bucket_name, s3_key)
                        logger.info(f"Uploaded {zip_file.name} -> s3://{bucket_name}/{s3_key}")
                    except Exception as e:
                        logger.error(f"Error uploading {zip_file.name}: {e}")
                    pbar.update(1)

    def upload_feed_file(self, bucket_name):
        """
        Upload the `feed.xml` file located in ./data/ to S3 under feeds/{year}/{month}.
        """
        feed_file_path = Path("./data/feed.xml")
        s3_feed_key = f"feeds/{self.extract_year}/{self.extract_month}/feed.xml"

        if feed_file_path.exists():
            try:
                self.s3_client.upload_file(str(feed_file_path), bucket_name, s3_feed_key)
                logger.info(f"Uploaded feed.xml -> s3://{bucket_name}/{s3_feed_key}")
            except Exception as e:
                logger.error(f"Error uploading feed.xml: {e}")
        else:
            logger.warning(f"feed.xml not found at {feed_file_path}. Skipping upload.")

    def list_extracted_files(self, bucket, prefix):
        """
        Return a set of file names (not full paths) that are already
        in S3 under the specified prefix. Helps skip duplicates.
        """
        existing = set()
        paginator = self.s3_client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                # Keep only the file name after the final slash
                existing_filename = obj["Key"].rsplit("/", 1)[-1]
                existing.add(existing_filename)
        return existing

    def process_all_zips(self, bucket_name):
        """
        1. Upload local zips to S3.
        2. Upload the `feed.xml` file to S3 under feeds/{year}/{month}.
        3. For each log_type, list the .zip files in S3.
        4. Process each ZIP file sequentially, but in parallel chunks internally.
        5. Print a summary of how many files were in each ZIP and how many were extracted.
        """
        # Step 1: Upload local zips
        self.upload_local_zips(bucket_name)

        # Step 2: Upload feed.xml
        self.upload_feed_file(bucket_name)

        # Step 3: Collect all .zip keys in S3
        all_zips = []
        for log_type, prefix_path in self.archive_paths.items():
            logger.info(f"Listing {log_type} zips under s3://{bucket_name}/{prefix_path}/...")
            paginator = self.s3_client.get_paginator("list_objects_v2")
            zip_keys = []
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix_path):
                for obj in page.get("Contents", []):
                    key = obj["Key"]
                    if key.endswith(".zip"):
                        zip_keys.append(key)
            if zip_keys:
                logger.info(f"Found {len(zip_keys)} {log_type} ZIP files in {prefix_path}.")
                for k in zip_keys:
                    all_zips.append((k, log_type))
            else:
                logger.warning(f"No .zip files found for {log_type} at {prefix_path}.")

        if not all_zips:
            logger.info("No ZIP files found at all. Nothing to process.")
            return

        # Process each ZIP file *sequentially*
        results = []
        for (zip_key, log_type) in all_zips:
            logger.info(f"\n=== Processing ZIP file: {zip_key} (log_type={log_type}) ===")
            total_files, extracted_files = self._process_single_zip(bucket_name, zip_key, log_type)
            logger.info(f"Completed {zip_key}: extracted {extracted_files}/{total_files} new files.")
            results.append((zip_key, log_type, total_files, extracted_files))

        # Final summary
        logger.info("\n=== FINAL SUMMARY ===")
        for (zip_key, log_type, total, extracted) in results:
            logger.info(
                f"ZIP: {zip_key} (type={log_type}) -> Total Files: {total}, Extracted: {extracted}"
            )
        logger.info("\nAll ZIP files have been processed successfully.")

    def _process_single_zip(self, bucket_name, zip_key, log_type):
        """
        Download the zip from S3, break it into chunks, and process each chunk in parallel.
        Returns (total_files_in_zip, newly_extracted_count).
        """
        try:
            logger.info(f"Downloading {zip_key} from s3://{bucket_name}/...")
            response = self.s3_client.get_object(Bucket=bucket_name, Key=zip_key)
            zip_data = response["Body"].read()

            with zipfile.ZipFile(io.BytesIO(zip_data), "r") as zf:
                all_files = zf.namelist()

            total_files = len(all_files)
            if not all_files:
                logger.warning(f"{zip_key} is empty or no files found inside.")
                return (0, 0)

            target_prefix = f"{self.extracted_base_path}/{self.extract_year}/{self.extract_month}/{log_type}/"
            existing = self.list_extracted_files(bucket_name, target_prefix)

            chunks = [
                all_files[i : i + self.chunk_size]
                for i in range(0, total_files, self.chunk_size)
            ]
            logger.info(f"{zip_key}: Found {total_files} files -> {len(chunks)} chunk(s).")

            extracted_count = 0

            def process_chunk(file_chunk):
                local_extracted = 0
                with zipfile.ZipFile(io.BytesIO(zip_data), "r") as local_zf:
                    for f_name in file_chunk:
                        if f_name in existing:
                            continue
                        file_data = local_zf.read(f_name)
                        target_key = f"{target_prefix}{f_name}"
                        self.s3_client.put_object(Bucket=bucket_name, Key=target_key, Body=file_data)
                        local_extracted += 1
                return local_extracted

            with ThreadPoolExecutor(max_workers=self.chunk_workers) as executor:
                futures = [executor.submit(process_chunk, chunk_list) for chunk_list in chunks]
                for fut in as_completed(futures):
                    extracted_count += fut.result()

            return total_files, extracted_count

        except Exception as e:
            logger.error(f"Error processing {zip_key}: {e}", exc_info=True)
            return (0, 0)


def main():
    bucket_name = "raw-data-bronze"
    extractor = ParallelChunkExtractor(chunk_size=10000, chunk_workers=8)
    extractor.process_all_zips(bucket_name)


if __name__ == "__main__":
    main()
