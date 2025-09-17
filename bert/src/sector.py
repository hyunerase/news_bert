import pandas as pd
import re

## 섹터 2차 분류

# 섹터-키워드 딕셔너리 반환
def load_sector_dict(keyword_csv_path: str) -> dict:
    df = pd.read_csv(keyword_csv_path)
    sector_dict = {}
    for _, row in df.iterrows():
        sector = row["섹터"]
        kw = str(row["키워드"]).strip()
        sector_dict.setdefault(sector, []).append(kw)
    return sector_dict


# 문장-섹터 분류 (단순 키워드 매칭)
def classify_sector(text: str, sector_dict: dict) -> str:
    scores = {sector: 0 for sector in sector_dict.keys()}
    for sector, keywords in sector_dict.items():
        for kw in keywords:
            if kw in str(text):
                scores[sector] += 1
    best_sector = max(scores, key=scores.get)
    return best_sector if scores[best_sector] > 0 else "분류불가"


# 뉴스-섹터 분류
def classify_news_csv(news_csv_path: str, keyword_csv_path: str, output_path: str):
    news_df = pd.read_csv(news_csv_path)
    sector_dict = load_sector_dict(keyword_csv_path)

    if "body_full" not in news_df.columns:
        raise ValueError("body_full이 존재하지 않음")

    def is_valid_text(text: str) -> bool:
        if not isinstance(text, str):
            return False
        return bool(re.search(r"[가-힣a-zA-Z0-9]", text))

    news_df = news_df[news_df["body_full"].apply(is_valid_text)].copy()
    
    news_df["섹터"] = news_df["body_full"].apply(lambda x: classify_sector(x, sector_dict))
    result_df = news_df[["body_full", "published_at_kst", "섹터"]]
    
    result_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"[섹터 분류 완료] {output_path}")
