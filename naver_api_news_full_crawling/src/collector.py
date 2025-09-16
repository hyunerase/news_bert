import time
import uuid
import random
import requests
from typing import Dict, Any, Iterator

from .config import NAVER_NEWS_URL, KST
from .utils import load_api_keys, strip_html_tags, parse_pubdate_to_kst, sha256_of_item
from .scraper import scrape_full_body

def request_news(query: str, display: int, start: int, sort: str, headers: Dict[str, Any]) -> Dict[str, Any]:
    """ Naver News API에 검색 요청 """
    params = {"query": query, "display": display, "start": start, "sort": sort}
    resp = requests.get(NAVER_NEWS_URL, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()

def harvest(query: str, max_items: int, sort: str, per_page: int) -> Iterator[Dict[str, Any]]:
    """ API 호출과 스크래핑을 조율하여 기사 데이터를 수집하는 제너레이터 """
    cid, csec = load_api_keys()
    api_headers = {"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": csec}
    
    total_fetched, start = 0, 1
    max_start = 1000

    while total_fetched < max_items and start <= max_start:
        time.sleep(random.uniform(0.2, 0.6))
        try:
            data = request_news(query, per_page, start, sort, api_headers)
        except requests.exceptions.RequestException as e:
            print(f"[warn] API 요청 실패, 재시도: {e}")
            time.sleep(5)
            continue

        items = data.get("items", [])
        if not items: break

        for item in items:
            scrape_url = item.get("originallink") or item.get("link")
            if not scrape_url: continue
            
            full_body, extractor = scrape_full_body(scrape_url, referer=item.get("link"))
            if not full_body:
                print(f"[info] 본문 추출 실패: {scrape_url} (방법: {extractor})")

            time.sleep(random.uniform(0.1, 0.3))

            yield {
                "id": str(uuid.uuid4()),
                "source": "naver_news_api",
                "query": query,
                "title": strip_html_tags(item.get("title", "")),
                "body_text": strip_html_tags(item.get("description", "")),
                "body_full": full_body,
                "extractor_used": extractor,
                "url": item.get("link"),
                "originallink": item.get("originallink"),
                "published_at_kst": parse_pubdate_to_kst(item.get("pubDate", "")),
                "first_seen_at_kst": time.strftime("%Y-%m-%d %H:%M:%S%z", time.gmtime()),
                "lang": "ko",
                "response_hash": sha256_of_item(item),
                "version": "v1"
            }
            total_fetched += 1
            if total_fetched >= max_items: break
        start += per_page