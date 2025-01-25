import os
import pandas as pd
from datetime import datetime
import boto3
import json
from collections import Counter
import nltk
from nltk.corpus import stopwords
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Download NLTK data
try:
    nltk.download('stopwords', quiet=True)
except Exception as e:
    logger.error(f"Failed to download NLTK stopwords: {e}")

# Initialize S3 and paths
s3 = boto3.client('s3')
now = datetime.now()
s3_path = f"s3://curated-data-gold/analytics/podcast_analytics_full/{now.year}/{now.month:02d}/{now.day:02d}/podcast_analytics_full.parquet"

def analyze_podcasts():
    """Load podcast data, analyze it, and return results dictionary plus DataFrame."""
    try:
        logger.info(f"Reading parquet file from: {s3_path}")
        df = pd.read_parquet(s3_path)
        
        df['pubdate'] = pd.to_datetime(df['pubdate'], format="%a, %d %b %Y %H:%M:%S %z", utc=True)
        df['hour'] = pd.to_datetime(df['time']).dt.hour

        stop_words = set(stopwords.words('english'))
        all_words = ' '.join(df['title'].astype(str)).lower().split()
        word_freq = Counter([w for w in all_words if w not in stop_words])

        analysis = {
            'top_listened': df.nlargest(10, 'listen_count')[['title', 'listen_count']],
            'top_liked': df.nlargest(10, 'like_count')[['title', 'like_count']],
            'top_searched': df.nlargest(10, 'search_count')[['title', 'search_count']],
            'release_interval': df.sort_values('pubdate')['pubdate'].diff().mean().days,
            'duration_stats': {
                'min': df['duration_seconds'].min() / 60,
                'max': df['duration_seconds'].max() / 60,
                'mean': df['duration_seconds'].mean() / 60,
                'median': df['duration_seconds'].median() / 60
            },
            'hourly_listens': df.groupby('hour')['listen_count'].sum().sort_values(ascending=False).to_dict(),
            'top_guest': df.nlargest(1, 'listen_count')['visitor'].iloc[0],
            'duration_listen_corr': df['duration_seconds'].corr(df['listen_count']),
            'top_words': dict(word_freq.most_common(25))
        }
        logger.info("Analysis completed successfully")
        return analysis, df
        
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}")
        raise

def save_analytics_results(results):
    """Save analysis results locally and upload to S3."""
    try:
        output_dir = "../analytics_results"
        date_path = f"{now.year}/{now.month:02d}/{now.day:02d}"
        report_dir = os.path.join(output_dir, "reports", date_path)
        os.makedirs(report_dir, exist_ok=True)

        files = {
            'analytics_summary.json': {k: v for k, v in results.items() if not isinstance(v, pd.DataFrame)},
            'top_listened.csv': results['top_listened'],
            'top_liked.csv': results['top_liked'],
            'top_searched.csv': results['top_searched']
        }

        for filename, content in files.items():
            local_path = os.path.join(report_dir, filename)
            if filename.endswith('.json'):
                with open(local_path, 'w', encoding='utf-8') as f:
                    json.dump(content, f, indent=4, ensure_ascii=False)
            else:
                content.to_csv(local_path, index=False)
            
            s3.upload_file(
                local_path,
                'curated-data-gold',
                f"analytics/reports/{date_path}/{filename}"
            )
            logger.info(f"Uploaded {filename} to S3")

        logger.info(f"All files saved locally to '{report_dir}' and uploaded to S3")

    except Exception as e:
        logger.error(f"Failed to save results: {str(e)}")
        raise

def print_results(results):
    """Print analysis results to console."""
    try:
        print("\nPodcast Analysis Results (TOP 10):")
        print("-" * 50)

        for metric in ['listened', 'liked', 'searched']:
            print(f"\nTop 10 Most {metric.title()}:")
            print(results[f'top_{metric}'])

        stats = results['duration_stats']
        print(f"\nAverage days between episodes: {results['release_interval']:.1f}")
        print(f"\nDuration (minutes):")
        print(f"  Min: {stats['min']:.1f}")
        print(f"  Max: {stats['max']:.1f}")
        print(f"  Average: {stats['mean']:.1f}")
        print(f"  Median: {stats['median']:.1f}")

        print("\nTop 5 Listening Hours:")
        for hour, count in sorted(results['hourly_listens'].items(), key=lambda x: x[1], reverse=True)[:5]:
            ampm = "AM" if hour < 12 else "PM"
            hour_12 = hour if hour <= 12 else hour - 12
            print(f"  {hour:02d}:00 ({hour_12:02d}:00 {ampm}): {count} listens")

        print(f"\nMost listened guest: {results['top_guest']}")
        print(f"Duration-Listens correlation: {results['duration_listen_corr']:.2f}")

        print("\nTop 25 Most Frequent Words:")
        for word, count in results['top_words'].items():
            print(f"  {word}: {count}")

    except Exception as e:
        logger.error(f"Failed to print results: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        results, df = analyze_podcasts()
        save_analytics_results(results)
        print_results(results)
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")
        raise