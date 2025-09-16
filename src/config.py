from datetime import timezone, timedelta

# API 및 스크래핑 설정
NAVER_NEWS_URL = "https://openapi.naver.com/v1/search/news.json"
KST = timezone(timedelta(hours=9))
BROWSER_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")