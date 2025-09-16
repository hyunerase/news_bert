import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification, pipeline


def run_bert_sentiment(input_csv: str, output_csv_sentiment: str, output_csv_stat: str):
    df = pd.read_csv(input_csv)

    model_name = "snunlp/KR-FinBert-SC"
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)
    nlp = pipeline("sentiment-analysis", model=model, tokenizer=tokenizer)

    # body_full 결측치 제거
    df["body_full"] = df["body_full"].fillna("").astype(str)

    # 배치 처리
    batch_size = 16
    labels, scores = [], []
    for i in range(0, len(df), batch_size):
        batch_texts = df["body_full"].iloc[i:i+batch_size].tolist()
        batch_results = nlp(batch_texts, truncation=True, max_length=512)
        labels.extend([r["label"] for r in batch_results])
        scores.extend([r["score"] for r in batch_results])

        print(f"[진행상황] {i + len(batch_results)}/{len(df)} 개 감성분석 완료")

    df["label"] = labels
    df["score"] = scores

    # date 처리
    if "published_at_kst" in df.columns:
        dt = pd.to_datetime(df["published_at_kst"], errors="coerce")
        dt = dt.dt.tz_convert(None) if dt.dt.tz is not None else dt
        df["date"] = dt.dt.date.astype(str)
    else:
        raise ValueError("date가 존재하지 않음")

    final_df = df[["date", "body_full", "섹터", "label", "score"]]
    final_df.to_csv(output_csv_sentiment, index=False, encoding="utf-8-sig")
    print(f"[감성분석 완료] {output_csv_sentiment}")

    # 집계
    agg_df = final_df.groupby(["date", "섹터", "label"])["score"].mean().reset_index()
    score_wide = agg_df.pivot_table(
        index=["date", "섹터"],
        columns="label",
        values="score",
        aggfunc="mean"
    ).reset_index().rename_axis(None, axis=1)

    count_df = (
        final_df.groupby(["date", "섹터", "label"])["body_full"]
        .count()
        .reset_index(name="count")
    )
    total_df = count_df.groupby(["date", "섹터"])["count"].sum().reset_index(name="total")
    count_df = count_df.merge(total_df, on=["date", "섹터"])
    count_df["percent"] = (count_df["count"] / count_df["total"] * 100).round(2)

    percent_wide = count_df.pivot_table(
        index=["date", "섹터"],
        columns="label",
        values="percent",
        aggfunc="mean"
    ).reset_index().rename_axis(None, axis=1)

    final_out = score_wide.merge(
        percent_wide, on=["date", "섹터"], suffixes=("_score", "_percent")
    )
    final_out.to_csv(output_csv_stat, index=False, encoding="utf-8-sig")
    print(f"[집계 완료] {output_csv_stat}")