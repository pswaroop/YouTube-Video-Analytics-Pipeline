# File: scrape_youtube_trending.py

from googleapiclient.discovery import build
import pandas as pd
import os
import json
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# === CONFIGURATION ===
API_KEY = os.getenv('YOUTUBE_DATA_API_KEY')
REGION = 'IN'  # You can change this to 'US', 'UK', 'CA', etc.
MAX_RESULTS = 50  # Max allowed per request
OUTPUT_DIR = 'data/raw/'


def get_trending_videos():
    youtube = build('youtube', 'v3', developerKey=API_KEY)

    request = youtube.videos().list(
        part='snippet,statistics,contentDetails',
        chart='mostPopular',
        maxResults=MAX_RESULTS,
        regionCode=REGION
    )

    response = request.execute()

    records = []
    for item in response.get("items", []):
        snippet = item.get("snippet", {})
        stats = item.get("statistics", {})
        content = item.get("contentDetails", {})

        record = {
            "video_id": item.get("id"),
            "title": snippet.get("title"),
            "channel_title": snippet.get("channelTitle"),
            "published_at": snippet.get("publishedAt"),
            "category_id": snippet.get("categoryId"),
            "tags": snippet.get("tags", []),
            "view_count": stats.get("viewCount", 0),
            "like_count": stats.get("likeCount", 0),
            "comment_count": stats.get("commentCount", 0),
            "duration": content.get("duration"),
            "trending_date": datetime.utcnow().strftime("%Y-%m-%d")
        }

        records.append(record)

    return records


def save_to_json(data, output_path):
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)
    print(f"Saved scraped data to: {output_path}")


def save_to_csv(data, output_path):
    df = pd.DataFrame(data)
    df.to_csv(output_path, index=False)
    print(f"Saved scraped data to: {output_path}")


def run_scraper(output_format='json'):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    data = get_trending_videos()
    today = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"youtube_trending_{REGION}_{today}.{output_format}"

    output_path = os.path.join(OUTPUT_DIR, filename)

    if output_format == 'json':
        save_to_json(data, output_path)
    else:
        save_to_csv(data, output_path)


if __name__ == "__main__":
    print("Started getting data from YouTube API")
    run_scraper(output_format='json')
    print("Scraped data successfully!")
