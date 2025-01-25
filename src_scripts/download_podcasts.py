import subprocess
from xml.etree import ElementTree as ET
import boto3
import requests
from datetime import datetime, timedelta
from pathlib import Path
from tqdm import tqdm
import logging
import json
import time
from concurrent.futures import ThreadPoolExecutor

class PodcastDownloader:
    def __init__(self, region_name='il-central-1', max_workers=1):
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.current_year = datetime.now().strftime('%Y')
        self.current_month = datetime.now().strftime('%m')
        self.session = requests.Session()
        self.setup_logging()
        self.total_downloaded_size = 0
        self.start_time = None
        self.error_files = []
        self.max_workers = max_workers

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO, 
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)

    def file_exists_in_s3(self, filename):
        try:
            s3_key = f'audio/podcasts/{self.current_year}/{self.current_month}/{filename}'
            self.s3_client.head_object(Bucket='raw-data-bronze', Key=s3_key)
            return True
        except self.s3_client.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    def read_xml_from_s3(self):
        try:
            response = self.s3_client.get_object(
                Bucket='raw-data-bronze',
                Key=f'feeds/2024/01/feed.xml'
            )
            return ET.ElementTree(ET.fromstring(response['Body'].read()))
        except Exception as e:
            self.logger.error(f"Error reading XML: {str(e)}")
            raise

    def get_audio_duration(self, file_path):
        try:
            result = subprocess.run(
                [
                    "ffprobe", "-i", str(file_path),
                    "-show_entries", "format=duration",
                    "-v", "quiet", "-of", "csv=p=0"
                ],
                capture_output=True,
                text=True,
                check=True
            )
            duration = float(result.stdout.strip())
            return duration
        except Exception as e:
            self.logger.error(f"Error determining audio duration: {str(e)}")
            raise

    def cut_audio_if_needed(self, input_path, output_path):
        try:
            duration = self.get_audio_duration(input_path)

            if duration > 3600:  # If audio is longer than 1 hour
                self.logger.info(f"Audio file exceeds 1 hour. Cutting into 1-hour segments: {input_path}")
                segment_start = 0
                segment_index = 0
                while segment_start < duration:
                    segment_end = min(segment_start + 3600, duration)
                    segment_output = output_path.parent / f"{output_path.stem}_part{segment_index}{output_path.suffix}"

                    # Use FFmpeg to cut the audio
                    subprocess.run([
                        "ffmpeg", "-i", str(input_path), "-ss", str(segment_start), "-to", str(segment_end), "-c", "copy", str(segment_output)
                    ], check=True)

                    self.logger.info(f"Segment created: {segment_output}")
                    segment_start += 3600
                    segment_index += 1

                return list(output_path.parent.glob(f"{output_path.stem}_part*{output_path.suffix}"))
            else:
                return [input_path]
        except Exception as e:
            self.logger.error(f"Error cutting audio file: {str(e)}")
            raise

    def download_and_upload(self, file_info):
        temp_path = Path("/tmp") / file_info['filename']
        try:
            self.logger.info(f"Starting download of {file_info['url']}")
            response = self.session.get(file_info['url'], stream=True, timeout=30)
            response.raise_for_status()

            with open(temp_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)

            file_size = temp_path.stat().st_size
            self.total_downloaded_size += file_size
            self.logger.info(f"Download complete: {file_size/1024/1024:.2f} MB")

            # Check and cut audio if needed
            output_path = Path("/tmp") / f"processed_{file_info['filename']}"
            audio_segments = self.cut_audio_if_needed(temp_path, output_path)

            for segment in audio_segments:
                s3_key = f'audio/podcasts/{self.current_year}/{self.current_month}/{segment.name}'
                with open(segment, 'rb') as f:
                    self.s3_client.put_object(
                        Bucket='raw-data-bronze',
                        Key=s3_key,
                        Body=f
                    )
                self.logger.info(f"Upload complete for {segment.name}")

            return True

        except Exception as e:
            self.logger.error(f"Error processing {file_info['filename']}: {str(e)}")
            self.error_files.append(file_info['filename'])
            return False
        finally:
            if temp_path.exists():
                temp_path.unlink()

    def create_download_report(self, successful_downloads, total_files, skipped_files):
        end_time = time.time()
        duration = timedelta(seconds=int(end_time - self.start_time))

        report = {
            "download_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "files": {
                "successful_downloads": successful_downloads,
                "total_files": total_files,
                "skipped_files": skipped_files,
                "failed_downloads": len(self.error_files),
                "failed_files": self.error_files
            },
            "data": {
                "total_size_bytes": self.total_downloaded_size,
                "total_size_mb": round(self.total_downloaded_size / (1024 * 1024), 2)
            },
            "duration": str(duration)
        }

        report_filename = f'download_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        report_key = f'audio/podcasts/download_logs/{self.current_year}/{self.current_month}/{report_filename}'

        try:
            self.s3_client.put_object(
                Bucket='raw-data-bronze',
                Key=report_key,
                Body=json.dumps(report, indent=4)
            )
            self.logger.info(f"Report saved: s3://raw-data-bronze/{report_key}")
        except Exception as e:
            self.logger.error(f"Error saving report: {str(e)}")

    def process_feed(self):
        self.start_time = time.time()
        tree = self.read_xml_from_s3()
        files_to_download = []
        skipped_files = 0

        for item in tree.getroot().find('channel').findall('item'):
            enclosure = item.find('enclosure')
            if enclosure is not None and 'url' in enclosure.attrib:
                url = enclosure.get('url')
                filename = url.split('/')[-1]

                if self.file_exists_in_s3(filename):
                    self.logger.info(f"Skipping existing file: {filename}")
                    skipped_files += 1
                    continue

                files_to_download.append({
                    'url': url,
                    'filename': filename
                })

        total_files = len(files_to_download) + skipped_files
        self.logger.info(f"Found {len(files_to_download)} new files")
        self.logger.info(f"Skipped {skipped_files} existing files")

        if not files_to_download:
            self.logger.info("No new files to download")
            self.create_download_report(0, total_files, skipped_files)
            return

        successful_downloads = 0

        def process_file(file_info):
            nonlocal successful_downloads
            if self.download_and_upload(file_info):
                successful_downloads += 1

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            list(tqdm(executor.map(process_file, files_to_download), total=len(files_to_download), desc="Downloading files"))

        self.logger.info(f"\nCompleted: {successful_downloads} of {len(files_to_download)} files")
        self.create_download_report(successful_downloads, total_files, skipped_files)

def main():
    downloader = PodcastDownloader()
    downloader.process_feed()

if __name__ == "__main__":
    main()

