from __future__ import annotations

from dotenv import load_dotenv
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

load_dotenv()

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

APIFY_TOKEN = os.getenv("APIFY_TOKEN")
APIFY_ACTOR_ID = os.getenv("APIFY_ACTOR_ID", "apify~facebook-groups-scraper")

RESULTS_AMOUNT = int(os.getenv("RESULTS_AMOUNT", "20"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "1"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "300"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
BACKOFF_BASE_SECONDS = float(os.getenv("BACKOFF_BASE_SECONDS", "3"))

HTTP_PROXY = os.getenv("HTTP_PROXY")
HTTPS_PROXY = os.getenv("HTTPS_PROXY")

VN_TZ = timezone(timedelta(hours=7))


def get_requests_session() -> requests.Session:
    session = requests.Session()

    if HTTP_PROXY or HTTPS_PROXY:
        session.proxies.update(
            {
                "http": HTTP_PROXY or "",
                "https": HTTPS_PROXY or "",
            }
        )

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            )
        }
    )

    return session


def backoff_sleep(attempt: int) -> None:
    delay = BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)) + random.uniform(0.5, 2.0)
    print(f"Retry sau {delay:.2f}s...")
    time.sleep(delay)


def crawl_group(group_url: str) -> List[Dict[str, Any]]:
    if not APIFY_TOKEN:
        raise ValueError("Thiếu APIFY_TOKEN.")

    api_url = (
        f"https://api.apify.com/v2/acts/{APIFY_ACTOR_ID}/run-sync-get-dataset-items"
    )

    payload = {
        "startUrls": [{"url": group_url}],
        "resultsLimit": RESULTS_AMOUNT,
        "viewOption": "CHRONOLOGICAL",
    }

    params = {
        "token": APIFY_TOKEN,
        "format": "json",
        "clean": "true",
    }

    session = get_requests_session()
    last_error: Optional[Exception] = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.post(
                api_url,
                params=params,
                json=payload,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code in (429, 500, 502, 503, 504):
                raise requests.HTTPError(f"HTTP {response.status_code} temporary error")

            response.raise_for_status()

            data = response.json()
            return data if isinstance(data, list) else []

        except Exception as exc:
            last_error = exc
            print(f"Lần thử {attempt}/{MAX_RETRIES} thất bại: {exc}")

            if attempt < MAX_RETRIES:
                backoff_sleep(attempt)

    raise RuntimeError(f"Crawl thất bại sau {MAX_RETRIES} lần. Lỗi cuối: {last_error}")


def parse_time(time_str: str) -> datetime:
    return datetime.fromisoformat(time_str.replace("Z", "+00:00"))


def calc_engagement(post: Dict[str, Any]) -> int:
    likes = int(post.get("likesCount", 0) or 0)
    comments = int(post.get("commentsCount", 0) or 0)
    shares = int(post.get("sharesCount", 0) or 0)
    return likes + comments + shares


def find_top_post_24h(posts: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    now_utc = datetime.now(timezone.utc)
    from_time = now_utc - timedelta(days=LOOKBACK_DAYS)

    filtered: List[Dict[str, Any]] = []

    for post in posts:
        time_str = post.get("time")
        if not time_str:
            continue

        try:
            post_time = parse_time(time_str)
        except Exception:
            continue

        if post_time < from_time:
            continue

        filtered.append(
            {
                "group_url": post.get("inputUrl", ""),
                "group_title": post.get("groupTitle", ""),
                "user": post.get("user", {}).get("name", ""),
                "time": post_time.isoformat(),
                "time_vn": post_time.astimezone(VN_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "url": post.get("url", ""),
                "text": post.get("text", ""),
                "likesCount": int(post.get("likesCount", 0) or 0),
                "commentsCount": int(post.get("commentsCount", 0) or 0),
                "sharesCount": int(post.get("sharesCount", 0) or 0),
                "engagement": calc_engagement(post),
            }
        )

    if not filtered:
        return None

    return max(filtered, key=lambda x: (x["engagement"], x["time"]))


def main() -> None:
    group_url = sys.argv[1] if len(sys.argv) > 1 else ""

    if not group_url:
        print("Thiếu group_url")
        sys.exit(1)

    result_item: Dict[str, Any] = {
        "crawl_time": datetime.now(VN_TZ).strftime("%Y-%m-%d %H:%M:%S"),
        "group_url": group_url,
        "group_title": "",
        "crawl_status": "",
        "error_message": "",
        "posts_fetched": 0,
        "has_top_post_24h": False,
        "top_post": None,
    }

    try:
        posts = crawl_group(group_url)
        result_item["posts_fetched"] = len(posts)

        if posts:
            result_item["group_title"] = posts[0].get("groupTitle", "")

        top_post = find_top_post_24h(posts)

        if top_post:
            result_item["crawl_status"] = "SUCCESS"
            result_item["has_top_post_24h"] = True
            result_item["group_title"] = top_post.get(
                "group_title", result_item["group_title"]
            )
            result_item["top_post"] = top_post
        else:
            result_item["crawl_status"] = "NO_POST_24H"

    except Exception as exc:
        result_item["crawl_status"] = "CRAWL_ERROR"
        result_item["error_message"] = str(exc)

    results = [result_item]

    print("JSON_RESULT_START")
    print(json.dumps(results, ensure_ascii=False))
    print("JSON_RESULT_END")


if __name__ == "__main__":
    main()
