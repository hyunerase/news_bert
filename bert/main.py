import os
import glob
from src.sector import classify_news_csv
from src.bert import run_bert_sentiment

if __name__ == "__main__":
    os.makedirs("out", exist_ok=True)
    
    files = glob.glob("../naver_api_news_full_crawling/out/*.csv")
    # 최신순 정렬
    files_sorted = sorted(files, key=os.path.getctime, reverse=True)

    keyword_csv = "src/11sector_keyword.csv"

    for i, news_csv in enumerate(files_sorted, start=1):
        print(f"\n[{i}/{len(files_sorted)}] 처리 중 → {news_csv}")

        base_name = os.path.splitext(os.path.basename(news_csv))[0]
        output_sector_csv = f"out/{base_name}_sector_mapping.csv"
        output_sentiment_csv = f"out/{base_name}_sentiment.csv"
        output_stat_csv = f"out/{base_name}_statistic.csv"

        # 뉴스 섹터 분류
        classify_news_csv(news_csv, keyword_csv, output_sector_csv)

        # 감성 분석, 집계
        run_bert_sentiment(output_sector_csv, output_sentiment_csv, output_stat_csv)

        print(f"완료: {base_name}")

    print("\n[모든 파일 처리 완료]")


"""
## 단일 분석 ##
if __name__ == "__main__":
    # 최신 뉴스 파일 업데이트
    files = glob.glob("../naver_api_news_full_crawling/out/*.csv")
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
"""
