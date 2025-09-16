import os
import glob
from src.sector import classify_news_csv
from src.bert import run_bert_sentiment

if __name__ == "__main__":
    # 뉴스 파일 업데이트
    files = glob.glob("../naver_api_news_full_crawling/out/*_금융.csv")
    latest_file = max(files, key=os.path.getctime)
    news_csv = latest_file

    keyword_csv = "src/11sector_keyword.csv"
    output_sector_csv = "out/news_sector_mapping.csv"

    output_sentiment_csv = "out/news_sentiment.csv"
    output_stat_csv = "out/sector_sentiment_statistic.csv"

    # 뉴스 섹터 분류
    classify_news_csv(news_csv, keyword_csv, output_sector_csv)

    # 감성 분석, 집계
    run_bert_sentiment(output_sector_csv, output_sentiment_csv, output_stat_csv)

    print("[모든 작업 완료]")
