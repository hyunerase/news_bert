import os
import html
import hashlib
from datetime import datetime
from typing import List, Dict, Any, Optional

from .config import KST

def load_api_keys():
    """ .env 파일에서 API 키를 로드하여 환경변수에 설정 """
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
    """ 스크래핑에 사용할 브라우저 헤더 생성 """
    from .config import BROWSER_UA
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

def strip_html_tags(s: str) -> str:
    """ HTML 태그와 엔티티를 제거/변환 """
    s = html.unescape(s or "")
    return s.replace("<b>", "").replace("</b>", "").strip()

def parse_pubdate_to_kst(pubdate: str) -> str:
    """ API에서 받은 날짜 문자열을 KST 시간대로 변환 """
    try:
        dt = datetime.strptime(pubdate, "%a, %d %b %Y %H:%M:%S %z")
        return dt.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S%z")
    except Exception:
        return datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S%z")

def sha256_of_item(item: Dict[str, Any]) -> str:
    """ API 응답 아이템의 해시값 계산 """
    raw = repr(sorted(item.items())).encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()

def dedupe(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """ URL과 제목 기준으로 기사 중복 제거 """
    seen = set()
    out = []
    for r in records:
        key = (r["url"], r["title"])
        if key in seen: continue
        seen.add(key)
        out.append(r)
    return out