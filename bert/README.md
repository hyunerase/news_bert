# 📌 프로젝트명
> 금융 뉴스 섹터 분류 및 감성 분석 파이프라인

---

## 소개
이 프로젝트는 금융 뉴스를 수집하여 아래의 과정을 자동화합니다.
1. 뉴스 본문 **산업 섹터별 분류**
2. **FinBERT-SC 모델**을 활용한 감성 분석
3. 섹터별 감성 점수를 **시계열 데이터로 집계**

---

## 폴더 구조
```
├── main.py # 메인 실행 스크립트
├── src/
│ ├── 11sector_keyword.csv # 섹터별 키워드
│ ├── sector.py # 섹터 분류 모듈
│ └── bert.py # 감성 분석 및 집계 모듈
├── requirements.txt # 패키지 종속성 목록
├── .gitignore # Git 제외 설정
├── out/
│ ├── news_sector_mapping.csv # 뉴스 → 섹터 분류 결과
│ ├── news_sentiment.csv # 기사별 감성 분석 결과
│ └── sector_sentiment_statistic.csv # 섹터별 감성 집계 결과
```

---

## 환경 설정

### 1. 가상환경 생성
```bash
python -m venv myvenv
source myvenv/bin/activate   # Mac/Linux
myvenv\Scripts\activate      # Windows
```

### 2. 라이브러리 설치
```bash
pip install -r requirements.txt
```

### 3. 실행 방법

1. **뉴스 데이터 준비**      
    뉴스 파일은 `../naver_api_news_full_crawling/out/*_금융.csv` 경로에 위치해야 합니다.

    필수 컬럼 : `body_full` (기사 본문), `published_at_kst` (발행일시)

2. **실행**
    ```bash
    python main.py
    ```

3. **처리 단계**

    섹터 분류 (sector.py)
    → `out/news_sector_mapping.csv` 생성

    감성 분석 (bert.py)
    → `out/news_sentiment.csv` 생성

    통계 집계 (bert.py)
    → `out/sector_sentiment_statistic.csv` 생성

### 4. 결과 예시
1. **섹터 분류 결과**

| body_full     | published_at_kst | 섹터   |
|---------------|------------------|--------|
| 예시 기사 본문 | 2025-09-16 08:00 | 정보기술 |

2. **감성 분석 결과**

| date       | 섹터 | label    | score |
|------------|------|----------|-------|
| 2025-09-16 | 금융 | positive | 0.87  |

3. **섹터 감성 집계**

| date       | 섹터 | positive_score | negative_score | neutral_score | positive_percent | negative_percent | neutral_percent |
|------------|------|----------------|----------------|---------------|------------------|------------------|-----------------|
| 2025-09-16 | 금융 | 0.76           | 0.12           | 0.12          | 70.0             | 15.0             | 15.0            |

---

## 참고
감성 분석 모델 : snunlp/KR-FinBert-SC

섹터 기준 : [GICS 11 Sector]