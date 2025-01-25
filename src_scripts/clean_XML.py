import xml.etree.ElementTree as ET
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import re
import json
import boto3
from datetime import datetime, timedelta
import io
import time
import logging

class FeedProcessor:
    def __init__(self, region_name='il-central-1'):
        self.s3_client = boto3.client('s3', region_name=region_name)
        self.current_year = datetime.now().strftime('%Y')
        self.current_month = datetime.now().strftime('%m')
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler()]
        )
        self.logger = logging.getLogger(__name__)

    def read_xml_from_s3(self, bucket, key):
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            xml_content = response['Body'].read()
            return ET.ElementTree(ET.fromstring(xml_content))
        except Exception as e:
            self.logger.error(f"Error reading XML from S3: {str(e)}")
            raise

    def write_to_s3(self, data, bucket, key):
        try:
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=data
            )
            self.logger.info(f"Successfully wrote to s3://{bucket}/{key}")
        except Exception as e:
            self.logger.error(f"Error writing to S3: {str(e)}")
            raise

    def convert_duration(self, duration_str):
        if pd.isna(duration_str):
            return pd.NaT
        try:
            parts = duration_str.split(':')
            if len(parts) == 3:
                h, m, s = map(int, parts)
            elif len(parts) == 2:
                h = 0
                m, s = map(int, parts)
            else:
                h = 0
                m = 0
                s = int(parts[0])
                
            total_seconds = h * 3600 + m * 60 + s
            if total_seconds > 3600:  # More than 1 hour
                self.logger.warning(f"Duration {duration_str} exceeds 1 hour, capping to 1 hour")
                return pd.Timedelta(hours=1)
                
            return pd.to_timedelta(f"{h}:{m}:{s}")
        except (ValueError, IndexError):
            return pd.NaT

    def create_processing_report(
        self,
        start_time,
        xml_episode_count,
        df_size,
        channel_df_size,
        silver_year,
        silver_month
    ):
        end_time = time.time()
        duration = timedelta(seconds=int(end_time - start_time))

        report = {
            "processing_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "data_date": {
                "year": silver_year,
                "month": silver_month
            },
            "statistics": {
                "episodes_in_xml": xml_episode_count,
                "processed_episodes": xml_episode_count,
                "successful_operations": {
                    "channel_parquet": True,
                    "feed_parquet": True
                }
            },
            "exported_files": {
                "channel": f"feeds/data/channel/{silver_year}/{silver_month}/channel.parquet",
                "feed": f"feeds/data/episodes/{silver_year}/{silver_month}/feed.parquet"
            },
            "data_sizes": {
                "feed_data_size_bytes": df_size,
                "feed_data_size_mb": round(df_size / (1024 * 1024), 2),
                "channel_data_size_bytes": channel_df_size,
                "channel_data_size_mb": round(channel_df_size / (1024 * 1024), 2),
                "total_data_size_bytes": df_size + channel_df_size,
                "total_data_size_mb": round((df_size + channel_df_size) / (1024 * 1024), 2)
            },
            "time": {
                "duration_seconds": int(end_time - start_time),
                "formatted_duration": str(duration)
            }
        }

        report_filename = f'processing_report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
        report_key = f'feeds/logs_reports/{report_filename}'

        try:
            self.write_to_s3(
                json.dumps(report, indent=4),
                'staging-data-silver',
                report_key
            )
            print(f"\nProcessing report saved to s3://staging-data-silver/{report_key}")
        except Exception as e:
            self.logger.error(f"Error saving processing report: {str(e)}")

    def clean_and_convert_rss(self):
        start_time = time.time()

        source_bucket = 'raw-data-bronze'
        target_bucket = 'staging-data-silver'
        xml_key = f'feeds/2024/01/feed.xml'

        tree = self.read_xml_from_s3(source_bucket, xml_key)
        root = tree.getroot()
        channel = root.find('channel')

        latest_date = None
        for item in channel.findall('item'):
            pub_date = item.find('pubDate')
            if pub_date is not None and pub_date.text:
                try:
                    date = datetime.strptime(pub_date.text, '%a, %d %b %Y %H:%M:%S %z')
                    if latest_date is None or date > latest_date:
                        latest_date = date
                except ValueError:
                    continue

        if latest_date is None:
            latest_date = datetime.now()

        silver_year = latest_date.strftime('%Y')
        silver_month = latest_date.strftime('%m')

        xml_episode_count = len(channel.findall('item'))
        self.logger.info(f"Number of episodes in XML: {xml_episode_count}")

        channel_data = {}
        for child in channel:
            tag = child.tag
            if tag.startswith('{http://www.itunes.com'):
                tag = tag.replace('{http://www.itunes.com/dtds/podcast-1.0.dtd}', 'itunes:')
            if tag == 'image':
                image_data = {}
                for image_child in child:
                    image_data[image_child.tag] = image_child.text
                channel_data[tag] = image_data
            elif tag.startswith('itunes:category'):
                if 'text' in child.attrib:
                    channel_data.setdefault('itunes:categories', []).append(child.attrib['text'])
                else:
                    for subcat in child:
                        if 'text' in subcat.attrib:
                            channel_data.setdefault('itunes:categories', []).append(subcat.attrib['text'])
            elif tag == 'itunes:owner':
                owner_data = {}
                for owner_child in child:
                    owner_data[owner_child.tag] = owner_child.text
                channel_data[tag] = owner_data
            else:
                text = child.text
                if text:
                    text = re.sub(r'\s+', ' ', text.strip())
                channel_data[tag] = text

        channel_df = pd.DataFrame([channel_data])
        channel_table = pa.Table.from_pandas(channel_df)
        channel_parquet_buffer = io.BytesIO()
        pq.write_table(channel_table, channel_parquet_buffer)
        channel_df_size = len(channel_parquet_buffer.getvalue())

        channel_parquet_key = f'feeds/data/channel/{silver_year}/{silver_month}/channel.parquet'
        self.write_to_s3(channel_parquet_buffer.getvalue(), target_bucket, channel_parquet_key)

        items = []
        for item in channel.findall('item'):
            item_data = {}
            for child in item:
                tag = child.tag
                if tag == '{http://purl.org/rss/1.0/modules/content/}encoded':
                    continue

                if tag.startswith('{http://www.itunes.com'):
                    tag = tag.replace('{http://www.itunes.com/dtds/podcast-1.0.dtd}', 'itunes:')

                if tag == 'enclosure':
                    item_data['enclosure_url'] = child.get('url')
                    item_data['enclosure_length'] = child.get('length')
                    item_data['enclosure_type'] = child.get('type')
                    if child.get('url'):
                        item_data['audio_filename'] = child.get('url').split('/')[-1]
                elif tag == 'itunes:image':
                    item_data[tag] = child.get('href')
                else:
                    text = child.text
                    if text:
                        text = re.sub(r'\s+', ' ', text.strip())
                    item_data[tag] = text

            title_episode_match = re.search(r'#(\d+)', item_data.get('title', '') or '')
            title_episode = int(title_episode_match.group(1)) if title_episode_match else None

            xml_episode_str = item_data.get('itunes:episode')
            if xml_episode_str:
                try:
                    xml_episode = int(xml_episode_str)
                except ValueError:
                    xml_episode = None
            else:
                xml_episode = None

            if title_episode and (not xml_episode or xml_episode != title_episode):
                item_data['itunes:episode'] = str(title_episode)

            items.append(item_data)

        df = pd.DataFrame(items)

        df['itunes:season'] = df['itunes:season'].fillna('1')
        df['itunes:episodeType'] = df['itunes:episodeType'].fillna('unknown')
        df['itunes:duration_td'] = df['itunes:duration'].apply(self.convert_duration)

        # Check for episodes over 1 hour
        over_hour = df[df['itunes:duration_td'] > pd.Timedelta(hours=1)]
        if not over_hour.empty:
            print("\nEpisodes with duration over 1 hour:")
            print(over_hour[['title', 'itunes:duration', 'itunes:duration_td']])

        df['itunes:episode'] = pd.to_numeric(df['itunes:episode'], errors='coerce').fillna(0)

        if 'itunes:title' in df.columns:
            df = df.drop(columns=['itunes:title'])

        df.columns = (
            df.columns
            .str.replace('itunes:', '', regex=False)
            .str.replace('episodeType', 'episodetype', regex=False)
            .str.replace('pubDate', 'pubdate', regex=False)
        )

        negative_episodes = df[df['episode'] < 0]
        if not negative_episodes.empty:
            print("\nRows with negative episode numbers:")
            print(negative_episodes)

        print("\nCount Validation:")
        print("----------------")
        print(f"Episodes in source XML: {xml_episode_count}")
        print(f"Episodes in processed parquet: {len(df)}")
        if xml_episode_count != len(df):
            print("WARNING: Source and processed counts don't match!")
            self.logger.warning(
                f"Count mismatch: XML has {xml_episode_count} episodes but parquet has {len(df)}"
            )
        else:
            print("✓ Source and processed counts match")

        print("\nChannel Parquet Sample:")
        print("----------------------")
        channel_sample = pd.read_parquet(io.BytesIO(channel_parquet_buffer.getvalue()))
        print(channel_sample.head().to_string())

        table = pa.Table.from_pandas(df)
        feed_parquet_buffer = io.BytesIO()
        pq.write_table(table, feed_parquet_buffer)
        df_size = len(feed_parquet_buffer.getvalue())

        print("\nFeed Parquet Sample:")
        print("-------------------")
        feed_sample = pd.read_parquet(io.BytesIO(feed_parquet_buffer.getvalue()))
        print("\nAll Columns:", feed_sample.columns.tolist())
        print("\nFirst row sample:")
        pd.set_option('display.max_columns', None)
        pd.set_option('display.width', None)
        pd.set_option('display.max_colwidth', None)
        print(feed_sample.head(1).to_string())

        print("\nTotal number of columns:", len(feed_sample.columns))
        print("Number of rows:", len(feed_sample))

        feed_parquet_key = f'feeds/data/episodes/{silver_year}/{silver_month}/feed.parquet'
        self.write_to_s3(feed_parquet_buffer.getvalue(), target_bucket, feed_parquet_key)

        self.create_processing_report(
            start_time,
            xml_episode_count,
            df_size,
            channel_df_size,
            silver_year,
            silver_month
        )

        self.logger.info("Successfully processed feed and uploaded to S3:")

def main():
    try:
        processor = FeedProcessor()
        processor.clean_and_convert_rss()
        print("Processing completed successfully!")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        raise

if __name__ == "__main__":
    main()