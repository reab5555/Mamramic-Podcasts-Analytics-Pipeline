import awswrangler as wr
import pandas as pd
import json
import boto3
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

now = datetime.now()

def check_data_quality(s3_path):
   expected_columns = [
       'title', 'pubdate', 'year', 'month', 'day', 'time',
       'description', 'author', 'duration', 'duration_seconds',
       'episode', 'episodetype', 'season', 'visitor',
       'search_count', 'listen_count', 'like_count'
   ]

   df = wr.s3.read_parquet(s3_path)
   print("\nData Quality Check Results:")
   print("-" * 50)

   quality_issues = {}

   # Check null values
   null_cols = {col: df[col].isnull().sum() for col in df.columns if df[col].isnull().any()}
   if null_cols:
       quality_issues['null_values'] = null_cols
       print("❌ Null Values:")
       for col, count in null_cols.items():
           print(f"   - {col}: {count} null values")
   else:
       print("✅ No null values found")

   # Check duration
   long_episodes = df[df['duration_seconds'] > 7200]
   if not long_episodes.empty:
       quality_issues['long_episodes'] = long_episodes[['episode', 'duration_seconds']].to_dict('records')
       print("\n❌ Duration Issues:")
       for _, row in long_episodes.iterrows():
           print(f"   - Episode {row['episode']}: {row['duration_seconds']} seconds")
   else:
       print("\n✅ All episodes within 1 hour limit")

   # Check uniqueness
   duplicate_titles = df[df.duplicated(['title'], keep=False)]
   if not duplicate_titles.empty:
       quality_issues['duplicate_titles'] = duplicate_titles['title'].value_counts().to_dict()
       print("\n❌ Duplicate Titles:")
       for title, count in quality_issues['duplicate_titles'].items():
           print(f"   - '{title}' appears {count} times")
   else:
       print("\n✅ All titles are unique")

   # Check episodes
   invalid_episodes = df[df['episode'] <= 0]
   if not invalid_episodes.empty:
       quality_issues['invalid_episodes'] = invalid_episodes['episode'].tolist()
       print(f"\n❌ Invalid Episode Numbers: {len(invalid_episodes)} episodes <= 0")

   duplicate_episodes = df[df.duplicated(['episode'], keep=False)]
   if not duplicate_episodes.empty:
       quality_issues['duplicate_episodes'] = duplicate_episodes['episode'].value_counts().to_dict()
       print("\n❌ Duplicate Episodes:")
       for ep, count in quality_issues['duplicate_episodes'].items():
           print(f"   - Episode {ep} appears {count} times")

   # Check sequence
   sorted_episodes = sorted(df['episode'].unique())
   missing_episodes = []
   for i in range(len(sorted_episodes) - 1):
       if sorted_episodes[i + 1] - sorted_episodes[i] != 1:
           missing_episodes.extend(range(sorted_episodes[i] + 1, sorted_episodes[i + 1]))

   if missing_episodes:
       quality_issues['missing_episodes'] = {
           'range': {'min': min(sorted_episodes), 'max': max(sorted_episodes)},
           'missing': missing_episodes
       }
       print("\n❌ Missing Episodes:")
       print(f"   Range: {min(sorted_episodes)} to {max(sorted_episodes)}")
       print(f"   Missing: {missing_episodes}")
   else:
       print(f"\n✅ Episodes continuous from {min(sorted_episodes)} to {max(sorted_episodes)}")

   # Check dates
   invalid_years = df[(df['year'] < 2015) | (df['year'] > 2025)]
   invalid_months = df[(df['month'] < 1) | (df['month'] > 12)]

   if not invalid_years.empty or not invalid_months.empty:
       quality_issues['invalid_dates'] = {
           'years': invalid_years[['episode', 'year']].to_dict('records'),
           'months': invalid_months[['episode', 'month']].to_dict('records')
       }
       if not invalid_years.empty:
           print("\n❌ Invalid Years:")
           for _, row in invalid_years.iterrows():
               print(f"   - Episode {row['episode']}: year = {row['year']}")
       if not invalid_months.empty:
           print("\n❌ Invalid Months:")
           for _, row in invalid_months.iterrows():
               print(f"   - Episode {row['episode']}: month = {row['month']}")
   else:
       print("\n✅ All date values valid")

   # Check columns
   if set(expected_columns) != set(df.columns):
       quality_issues['column_mismatch'] = {
           'expected': expected_columns,
           'actual': df.columns.tolist(),
           'missing': [col for col in expected_columns if col not in df.columns],
           'unexpected': [col for col in df.columns if col not in expected_columns]
       }
       print("\n❌ Column mismatch:")
       print("Expected:", sorted(expected_columns))
       print("Actual:", sorted(df.columns))
   else:
       print("\n✅ All expected columns present")

   return quality_issues, df

def save_quality_report(quality_issues, df, s3_path):
   now = datetime.now()
   report_name = f"quality_report_{now.year}_{now.month:02d}_{now.day:02d}.json"
   report_path = f"data_quality_reports/podcast_analytics/{now.year}/{now.month:02d}/{now.day:02d}/{report_name}"

   report = {
       "timestamp": now.isoformat(),
       "source_file": s3_path,
       "total_records": len(df),
       "quality_issues": quality_issues if quality_issues else "No issues found"
   }

   s3 = boto3.client('s3')
   s3.put_object(
       Bucket='curated-data-gold',
       Key=report_path,
       Body=json.dumps(report, indent=2, default=str)
   )
   print(f"Report saved to s3://curated-data-gold/{report_path}")

def quality_verify_task():
   now = datetime.now()
   s3_path = f"s3://curated-data-gold/analytics/podcast_analytics_full/{now.year}/{now.month:02d}/{now.day:02d}/podcast_analytics_full.parquet"
   quality_issues, df = check_data_quality(s3_path)
   save_quality_report(quality_issues, df, s3_path)

if __name__ == "__main__":
   quality_verify_task()