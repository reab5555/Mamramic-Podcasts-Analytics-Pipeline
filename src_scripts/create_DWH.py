import pandas as pd
import boto3
from datetime import datetime
import re
from tqdm import tqdm
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_latest_feed():
    s3 = boto3.client('s3')
    bucket = 'staging-data-silver'
    response = s3.list_objects_v2(Bucket=bucket, Prefix='feeds/')
    feed_files = [obj for obj in response['Contents'] if obj['Key'].endswith('feed.parquet')]
    latest_file = max(feed_files, key=lambda x: x['LastModified'])
    logger.info(f"Latest feed file: {latest_file['Key']}")
    return pd.read_parquet(f"s3://{bucket}/{latest_file['Key']}")

def get_latest_logs():
    s3 = boto3.client('s3')
    bucket = 'staging-data-silver'
    all_files = []
    response = s3.list_objects_v2(Bucket=bucket, Prefix='logs/data/')

    for obj in response.get('Contents', []):
        if '100k_logs.parquet' in obj['Key'] or '30k_logs.parquet' in obj['Key']:
            all_files.append(obj)

    if not all_files:
        raise FileNotFoundError("No log parquet files found")

    logs_100k = max((obj for obj in all_files if '100k_logs.parquet' in obj['Key']),
                    key=lambda x: x['LastModified'])
    logs_30k = max((obj for obj in all_files if '30k_logs.parquet' in obj['Key']),
                   key=lambda x: x['LastModified'])

    logger.info(f"Latest log files:\n100k: {logs_100k['Key']}\n30k: {logs_30k['Key']}")

    logs_100k_df = pd.read_parquet(f"s3://{bucket}/{logs_100k['Key']}")
    logs_30k_df = pd.read_parquet(f"s3://{bucket}/{logs_30k['Key']}")

    # Validation checks
    for df, name in [(logs_100k_df, '100k'), (logs_30k_df, '30k')]:
        logger.info(f"\nValidating {name} logs:")
        logger.info(f"Total rows: {len(df)}")
        logger.info(f"Null values: {df.isnull().sum().sum()}")
        logger.info(f"Unique episodes: {df['episode_number'].nunique()}")
        logger.info(f"Unique events: {df['event'].unique()}")
        logger.info(f"Date range: {df['timestamp'].min()} to {df['timestamp'].max()}")

    return logs_100k_df, logs_30k_df

def extract_visitor(title):
    pattern = r'MamraMic#\d+\s*-\s*(.*)'
    match = re.search(pattern, title)
    if match:
        return match.group(1).strip()
    return None

def process_logs(logs_df):
    pre_rows = len(logs_df)
    pre_events = logs_df['event'].value_counts().to_dict()

    grouped = logs_df.groupby(['episode_number', 'event'])['unique_id'].count().reset_index()
    pivot_df = grouped.pivot(
        index='episode_number',
        columns='event',
        values='unique_id'
    ).reset_index()

    pivot_df = pivot_df.rename(columns={
        'search': 'search_count',
        'listen': 'listen_count',
        'like': 'like_count'
    }).fillna(0)

    total_events = sum(pivot_df[['search_count', 'listen_count', 'like_count']].sum())
    logger.info(f"\nValidation Results:")
    logger.info(f"Original rows: {pre_rows}")
    logger.info(f"Original events: {pre_events}")
    logger.info(f"Total events after pivot: {total_events}")

    if total_events != pre_rows:
        logger.warning("Event count mismatch detected!")

    return pivot_df

def validate_and_clean_data(df):
    start_time = datetime.now()

    initial_nulls = df.isnull().sum()
    initial_duplicates = df['episode'].duplicated().sum()
    initial_size = df.memory_usage().sum() / (1024 * 1024)

    logger.info(f"""
  Initial Stats:
  - Null values per column: {initial_nulls[initial_nulls > 0].to_dict()}
  - Duplicate episodes: {initial_duplicates}
  - Memory usage: {initial_size:.2f} MB
  - Total columns: {len(df.columns)}
  """)

    null_rows = df[df[['episode_number', 'like_count', 'listen_count', 'search_count']].isnull().any(axis=1)]
    if not null_rows.empty:
        logger.info(
            f"Removing rows with null values:\n{null_rows[['episode', 'title', 'episode_number', 'like_count', 'listen_count', 'search_count']]}")
        df = df.dropna(subset=['episode_number', 'like_count', 'listen_count', 'search_count'])

    required_fields = [
        'title', 'pubdate', 'year', 'month', 'day', 'time', 'description', 'author',
        'duration', 'duration_seconds', 'episode', 'episodetype',
        'season', 'visitor', 'search_count', 'listen_count', 'like_count'
    ]

    df = df[required_fields].copy()
    df['visitor'] = df['visitor'].fillna('Unknown')

    df['title'] = df['title'].astype(str)
    df['pubdate'] = df['pubdate'].astype(str)
    df['description'] = df['description'].astype(str)
    df['author'] = df['author'].astype(str)
    df['duration'] = df['duration'].astype(str)
    df['duration_seconds'] = df['duration_seconds'].astype('int64')
    df['episode'] = df['episode'].astype('int64')
    df['episodetype'] = df['episodetype'].astype(str)
    df['season'] = df['season'].astype('int64')
    df['visitor'] = df['visitor'].astype(str)
    df['year'] = df['year'].astype('int64')
    df['month'] = df['month'].astype('int64')
    df['day'] = df['day'].astype('int64')
    df['time'] = df['time'].astype(str)
    df[['search_count', 'listen_count', 'like_count']] = df[['search_count', 'listen_count', 'like_count']].astype('int64')

    end_time = datetime.now()
    final_size = df.memory_usage().sum() / (1024 * 1024)

    logger.info(f"""
  Final Stats:
  - Processing time: {(end_time - start_time).total_seconds():.2f} seconds
  - Memory usage: {final_size:.2f} MB
  - Total columns: {len(df.columns)}
  """)
    return df

def duration_to_seconds_capped(duration):
    """
    Convert HH:MM:SS or MM:SS into total seconds and cap at 3600 seconds (1 hour).
    """
    parts = duration.split(':')
    if len(parts) == 3:
        hours, minutes, seconds = map(int, parts)
        total_seconds = hours * 3600 + minutes * 60 + seconds
    elif len(parts) == 2:
        minutes, seconds = map(int, parts)
        total_seconds = minutes * 60 + seconds
    else:
        # Handle unexpected formats (or set total_seconds = 0, etc.)
        total_seconds = 0

    # Cap any duration over 1 hour to exactly 1 hour (3600 seconds)
    total_seconds = min(total_seconds, 3600)
    return total_seconds

def keep_or_cap_duration_str(duration):
    """
    Convert the original duration string into "HH:MM:SS" format,
    capped at 1 hour if total exceeds 3600 seconds.
    """
    total_seconds = duration_to_seconds_capped(duration)
    hours = total_seconds // 3600
    remainder = total_seconds % 3600
    minutes = remainder // 60
    seconds = remainder % 60
    # Return a HH:MM:SS string
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def process_data():
    feed_df = get_latest_feed()
    logger.info(f"Feed columns: {feed_df.columns.tolist()}")

    # Drop columns that are not needed
    feed_df = feed_df.drop([
        'image', 'link', 'comments', 'guid', 'enclosure_url',
        'enclosure_length', 'enclosure_type', 'summary', 'explicit',
        'block', 'duration_td'
    ], axis=1, errors='ignore')

    feed_df['visitor'] = feed_df['title'].apply(extract_visitor)

    # Convert 'pubdate' to datetime (UTC) and extract date/time parts
    feed_df['pubdate_dt'] = pd.to_datetime(feed_df['pubdate'], format="%a, %d %b %Y %H:%M:%S %z", utc=True)
    feed_df['year'] = feed_df['pubdate_dt'].dt.year
    feed_df['month'] = feed_df['pubdate_dt'].dt.month
    feed_df['day'] = feed_df['pubdate_dt'].dt.day
    feed_df['time'] = feed_df['pubdate_dt'].dt.strftime('%H:%M:%S')
    feed_df.drop('pubdate_dt', axis=1, inplace=True)

    # Convert duration to capped seconds
    feed_df['duration_seconds'] = feed_df['duration'].apply(duration_to_seconds_capped)

    # (Optional) also modify the text duration to reflect the 1-hour cap
    feed_df['duration'] = feed_df['duration'].apply(keep_or_cap_duration_str)

    # Convert episode and season to int
    feed_df['episode'] = feed_df['episode'].astype(int)
    feed_df['season'] = feed_df['season'].astype(int)

    # Combine logs
    logs_100k, logs_30k = get_latest_logs()
    logs_df = pd.concat([logs_100k, logs_30k])
    event_counts = process_logs(logs_df)
    event_counts['episode_number'] = event_counts['episode_number'].astype(int)

    # Merge feed and logs
    final_df = pd.merge(feed_df, event_counts, left_on='episode', right_on='episode_number', how='left')

    # Clean up final data
    return validate_and_clean_data(final_df)

def create_dimensional_model(df):
    dim_tables = {
        'dim_date': df[['year', 'month', 'day', 'time']].drop_duplicates(),
        'dim_episode': df[[
            'episode', 'title', 'description', 'episodetype',
            'season', 'duration', 'duration_seconds'
        ]].drop_duplicates(),
        'dim_visitor': df[['visitor']].drop_duplicates(),
        'dim_author': df[['author']].drop_duplicates(),
        'fact_engagement': df[[
            'episode', 'visitor', 'author', 'year', 'month', 'day',
            'search_count', 'listen_count', 'like_count'
        ]]
    }

    for table_name, table_df in dim_tables.items():
        logger.info(f"{table_name} size: {len(table_df)} rows")

    return dim_tables

def save_to_s3(df, path, table_type):
    base_path = {
        'fact': 'facts/engagement',
        'dim': 'dimensions',
        'analytics': 'analytics/podcast_analytics_full'
    }

    now = datetime.now()
    date_path = f"{now.year}/{now.month:02d}/{now.day:02d}"

    if table_type == 'dim':
        # e.g., s3://curated-data-gold/dimensions/dim_episode/2025/01/25/dim_episode.parquet
        base = f"{base_path[table_type]}/{path}/{date_path}/{path}"
    else:
        # e.g., s3://curated-data-gold/facts/engagement/2025/01/25/fact_engagement.parquet
        # or s3://curated-data-gold/analytics/podcast_analytics_full/2025/01/25/podcast_analytics_full.parquet
        base = f"{base_path[table_type]}/{date_path}/{path}"

    parquet_path = f"s3://curated-data-gold/{base}.parquet"
    df.to_parquet(parquet_path, index=False, engine='pyarrow')

    s3 = boto3.client('s3')
    try:
        parquet_response = s3.head_object(
            Bucket='curated-data-gold',
            Key=parquet_path.replace('s3://curated-data-gold/', '')
        )
        parquet_size = parquet_response['ContentLength'] / (1024 * 1024)
        logger.info(f"Saved {base}:")
        logger.info(f"- Parquet: {parquet_size:.2f} MB")
    except:
        logger.warning(f"Could not get file sizes for {base}")

def main():
    start_time = datetime.now()
    try:
        logger.info("Starting ETL process...")
        with tqdm(total=5) as pbar:
            final_df = process_data()
            pbar.update(1)

            # Save the full analytics file
            save_to_s3(final_df, 'podcast_analytics_full', 'analytics')
            pbar.update(1)

            # Create & save dimension/fact tables
            dim_tables = create_dimensional_model(final_df)
            pbar.update(1)

            # Fact table
            save_to_s3(dim_tables['fact_engagement'], 'fact_engagement', 'fact')

            # Dimensions
            for table_name, df_dim in dim_tables.items():
                if table_name != 'fact_engagement':
                    save_to_s3(df_dim, table_name, 'dim')
            pbar.update(2)

        end_time = datetime.now()
        logger.info(
            f"ETL Process Summary:\n"
            f"- Total time: {(end_time - start_time).total_seconds():.2f} seconds\n"
            f"- Files generated: {len(dim_tables) + 1}"
        )
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        raise

if __name__ == '__main__':
    main()
