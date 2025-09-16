import csv
from pathlib import Path
from datetime import datetime, timedelta

# src 폴더의 함수들을 가져옴
from src.config import KST
from src.collector import harvest
from src.utils import dedupe

def save_to_csv(records: list, path: str):
    """ 수집된 데이터를 CSV 파일로 저장 """
    if not records: return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fields = records[0].keys()
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        wr = csv.DictWriter(f, fieldnames=fields)
        wr.writeheader()
        wr.writerows(records)

def save_to_parquet(records: list, path: str):
    """ 수집된 데이터를 Parquet 파일로 저장 """
    try:
        import pandas as pd
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(records)
        df.to_parquet(path, index=False)
    except Exception as e:
        print(f"[warn] Parquet 저장 실패: {e}")

def main():
    """ 메인 실행 함수 """
    # ---- 수집 설정 ----
    QUERY = "금융 AI"
    MAX_ITEMS = 1000
    RECENT_DAYS_LIMIT = 30
    SORT_ORDER = "date" # 최신순(date) 또는 관련도순(sim)
    
    print(f"'{QUERY}' 키워드로 최신 기사 수집을 시작합니다 (최대 {MAX_ITEMS}건).")
    
    all_recs = []
    try:
        # 1. 데이터 수집 (KeyboardInterrupt를 감지하기 위해 list() 대신 for 루프 사용)
        print("수집을 중단하려면 Ctrl+C를 누르세요...")
        for record in harvest(query=QUERY, max_items=MAX_ITEMS, sort=SORT_ORDER, per_page=100):
            all_recs.append(record)
            # 실시간 진행 상황을 보기 위한 출력 (10개마다)
            if len(all_recs) % 10 == 0:
                print(f"  현재까지 {len(all_recs)}건 수집됨...")

    except KeyboardInterrupt:
        print("\n사용자에 의해 수집이 중단되었습니다. 현재까지 수집된 데이터로 저장을 시도합니다.")

    if not all_recs:
        print("수집된 기사가 없습니다.")
        return

    # 2. 중복 제거
    deduped_recs = dedupe(all_recs)
    print(f"수집 건수(중복 제거 전): {len(all_recs)}, (중복 제거 후): {len(deduped_recs)}")

    # 3. 날짜 필터링
    if RECENT_DAYS_LIMIT > 0:
        print(f"발행일 기준 최근 {RECENT_DAYS_LIMIT}일 이내 기사만 필터링합니다.")
        limit_date = datetime.now(KST) - timedelta(days=RECENT_DAYS_LIMIT)
        final_recs = [r for r in deduped_recs if datetime.strptime(r["published_at_kst"], "%Y-%m-%d %H:%M:%S%z") >= limit_date]
        print(f"필터링 후 최종 건수: {len(final_recs)}")
    else:
        final_recs = deduped_recs

    # 4. 파일 저장
    if final_recs:
        timestamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
        safe_query = "".join(c for c in QUERY if c.isalnum() or c in " ").strip().replace(" ", "_")
        
        # 중단된 경우 파일명에 'incomplete' 추가
        status_tag = "" if len(all_recs) >= MAX_ITEMS else "_incomplete"
        base_filename = f"out/{timestamp}_{safe_query}{status_tag}"
        
        csv_path = f"{base_filename}.csv"
        parquet_path = f"{base_filename}.parquet"

        save_to_csv(final_recs, csv_path)
        save_to_parquet(final_recs, parquet_path)
        print(f"저장 완료: {csv_path}, {parquet_path}")
    else:
        print("저장할 기사가 없습니다.")

if __name__ == "__main__":
    main()