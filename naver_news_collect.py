# 본 파일은 무시해주세요
import os, csv, time, uuid, hashlib, random, math, html
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Iterator, Tuple, Optional
import requests
from urllib.parse import quote, urlparse
from pathlib import Path
from bs4 import BeautifulSoup

# --- 라이브러리 임포트 (없으면 경고만 출력) ---
try:
    import trafilatura
except ImportError:
    trafilatura = None
try:
    from readability import Document
except ImportError:
    Document = None

# --- 설정 ---
NAVER_NEWS_URL = "https://openapi.naver.com/v1/search/news.json"
KST = timezone(timedelta(hours=9))
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# .env에서 키 로드
def load_api_keys():
    from pathlib import Path
    env_path = Path(".env")
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            if not line.strip() or line.strip().startswith("#"): continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())
    cid = os.getenv("NAVER_CLIENT_ID")
    csec = os.getenv("NAVER_CLIENT_SECRET")
    if not cid or not csec:
        raise RuntimeError("환경변수 NAVER_CLIENT_ID / NAVER_CLIENT_SECRET 가 필요합니다 (.env 사용 가능)")
    return cid, csec

def get_browser_headers(referer: Optional[str] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": BROWSER_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    if referer:
        headers["Referer"] = referer
    return headers

def scrape_full_body(url: str, referer: Optional[str] = None) -> Tuple[str, str]:
    """
    다중 레이어 방식으로 기사 본문을 추출합니다.
    반환값: (본문 텍스트, 사용된 추출기 이름)
    """
    try:
        # SSL 오류 발생 시, 검증을 비활성화하고 재시도하기 위한 세션
        session = requests.Session()
        headers = get_browser_headers(referer)
        
        try:
            resp = session.get(url, headers=headers, timeout=10, allow_redirects=True)
        except requests.exceptions.SSLError:
            # SSL 오류 발생 시, 인증서 검증을 비활성화하고 재시도
            resp = session.get(url, headers=headers, timeout=10, allow_redirects=True, verify=False)
        
        resp.raise_for_status()
        
        # 1순위: trafilatura (가장 성능 좋음)
        if trafilatura:
            text = trafilatura.extract(resp.text, include_comments=False, include_tables=False)
            if text and len(text) > 100:
                return text, "trafilatura"

        # 2순위: readability-lxml
        if Document:
            doc = Document(resp.text)
            html_summary = doc.summary()
            soup = BeautifulSoup(html_summary, "lxml")
            text = soup.get_text(separator="\n", strip=True)
            if text and len(text) > 100:
                return text, "readability"

        # 3순위: 최후의 수단 (body 전체 텍스트)
        soup = BeautifulSoup(resp.text, "lxml")
        if soup.body:
            return soup.body.get_text(separator="\n", strip=True), "fallback_body"
        
        return "", "extract_failed"

    except Exception as e:
        error_msg = str(e)
        if "403" in error_msg:
            return "", "blocked_403"
        return "", f"error_{type(e).__name__}"

def strip_html_tags(s: str) -> str:
    s = html.unescape(s or "")
    return s.replace("<b>", "").replace("</b>", "").strip()

def parse_pubdate_to_kst(pubdate: str) -> str:
    try:
        dt = datetime.strptime(pubdate, "%a, %d %b %Y %H:%M:%S %z")
        return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S%z")
    except Exception:
        return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S%z")

def sha256_of_item(item: Dict[str, Any]) -> str:
    raw = repr(sorted(item.items())).encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()

def request_news(query: str, display: int, start: int, sort: str, headers: Dict[str, str]) -> Dict[str, Any]:
    params = {"query": query, "display": display, "start": start, "sort": sort}
    resp = requests.get(NAVER_NEWS_URL, headers=headers, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()

def harvest(query: str, max_items: int = 200, sort: str = "date", per_page: int = 100) -> Iterator[Dict[str, Any]]:
    cid, csec = load_api_keys()
    api_headers = {"X-Naver-Client-Id": cid, "X-Naver-Client-Secret": csec}
    
    total_fetched = 0
    start = 1
    per_page = max(1, min(100, per_page))
    max_start = 1000

    while total_fetched < max_items and start <= max_start:
        time.sleep(random.uniform(0.2, 0.6))
        try:
            data = request_news(query=query, display=per_page, start=start, sort=sort, headers=api_headers)
        except requests.exceptions.RequestException as e:
            print(f"[warn] API 요청 실패, 재시도: {e}")
            time.sleep(5)
            continue

        items = data.get("items", [])
        if not items: break

        for item in items:
            scrape_url = item.get("originallink") or item.get("link")
            if not scrape_url: continue
            
            # Referer로 네이버 뉴스 링크를 전달하여 성공률 향상
            naver_link_referer = item.get("link")
            full_body, extractor = scrape_full_body(scrape_url, referer=naver_link_referer)
            
            if not full_body:
                print(f"[info] 본문 추출 실패: {scrape_url} (방법: {extractor})")

            time.sleep(random.uniform(0.1, 0.3))

            rec = {
                "id": str(uuid.uuid4()),
                "source": "naver_news_api",
                "query": query,
                "title": strip_html_tags(item.get("title", "")),
                "body_text": strip_html_tags(item.get("description", "")),
                "body_full": full_body,
                "extractor_used": extractor, # 추출 방법 기록
                "url": item.get("link"),
                "originallink": item.get("originallink"),
                "published_at_kst": parse_pubdate_to_kst(item.get("pubDate", "")),
                "first_seen_at_kst": datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S%z"),
                "lang": "ko",
                "response_hash": sha256_of_item(item),
                "version": "v1"
            }
            yield rec
            total_fetched += 1
            if total_fetched >= max_items: break
        start += per_page

def dedupe(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for r in records:
        key = (r["url"], r["title"])
        if key in seen: continue
        seen.add(key)
        out.append(r)
    return out

def save_csv(records: List[Dict[str, Any]], path: str):
    if not records: return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    # 필드에 extractor_used 추가
    fields = ["id","source","query","title","body_text","body_full","extractor_used","url","originallink",
              "published_at_kst","first_seen_at_kst","lang","response_hash","version"]
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        wr = csv.DictWriter(f, fieldnames=fields)
        wr.writeheader()
        for r in records:
            wr.writerow(r)

def save_parquet(records: List[Dict[str, Any]], path: str):
    try:
        import pandas as pd
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(records)
        df.to_parquet(path, index=False)
    except Exception as e:
        print(f"[warn] Parquet 저장 실패: {e}")

if __name__ == "__main__":
    QUERY = "바이오 헬스 주식투자"
    MAX_ITEMS = 200 # 테스트를 위해 줄여서 실행 권장
    RECENT_DAYS_LIMIT = 100
    SORT_ORDER = "date"

    print(f"'{QUERY}' 키워드로 최신 기사 수집을 시작합니다 (최대 {MAX_ITEMS}건).")
    recs = list(harvest(QUERY, max_items=MAX_ITEMS, sort=SORT_ORDER, per_page=100))
    
    recs_deduped = dedupe(recs)
    print(f"수집 건수(중복 제거 전): {len(recs)}, (중복 제거 후): {len(recs_deduped)}")

    if RECENT_DAYS_LIMIT > 0:
        print(f"발행일 기준 최근 {RECENT_DAYS_LIMIT}일 이내 기사만 필터링합니다.")
        today = datetime.now(KST)
        limit_date = today - timedelta(days=RECENT_DAYS_LIMIT)
        
        filtered_recs = [r for r in recs_deduped if datetime.strptime(r["published_at_kst"], "%Y-%m-%d %H:%M:%S%z") >= limit_date]
        
        print(f"필터링 후 최종 건수: {len(filtered_recs)}")
        final_recs = filtered_recs
    else:
        final_recs = recs_deduped

    if final_recs:
        timestamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
        safe_query = "".join(c for c in QUERY if c.isalnum() or c in " ").strip().replace(" ", "_")
        
        base_filename = f"{timestamp}_{safe_query}"
        csv_path = f"out/{base_filename}.csv"
        parquet_path = f"out/{base_filename}.parquet"

        save_csv(final_recs, csv_path)
        save_parquet(final_recs, parquet_path)
        print(f"saved: {csv_path} , {parquet_path}")
    else:
        print("저장할 기사가 없습니다.")
