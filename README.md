# Binance USDS-M 데이터 수집기

이 레포는 Binance USDS-M 선물 퍼블릭 REST API를 이용해 최근 데이터를 수집하고 CSV로 저장하는 자동화 파이프라인을 제공합니다. GitHub Actions에서 스케줄(15분 간격)과 수동 실행(workflow_dispatch)을 모두 지원하며, 수집 시각은 항상 한국 시간(KST) 기준으로 기록됩니다.

## 구성 요소
- `collector.py`: 모든 엔드포인트를 순회하며 CSV를 생성하는 스크립트입니다.
- `.github/workflows/binance-collector.yml`: GitHub Actions 워크플로. Python 3.11 환경에서 스크립트를 실행하고 결과를 커밋/푸시합니다.
- `requirements.txt`: 실행에 필요한 최소 의존성(`requests`, `pandas`).
- `data/`: 수집된 CSV가 저장되는 기본 디렉터리(자동 생성, `.gitignore` 처리).

## 주요 변수 수정 방법
`collector.py` 상단의 상수로 기본값을 정의합니다. 필요 시 해당 값만 수정해 재배포할 수 있도록 했습니다.

```python
SYMBOLS = ["ARBUSDT", "BTCUSDT", "ETHUSDT"]  # 수집 대상 심볼
INTERVAL = "15m"                               # Kline/Index/Mark/Premium 간격
DAYS = 30                                       # 조회 기간(일)
OUT_DIR = "data"                                # CSV 저장 디렉터리
OI_PERIOD = "1h"                               # Open Interest 통계 주기
RATIO_PERIOD = "1h"                            # Long/Short, Taker Vol 주기
```

이 외 파라미터(예: `MAX_KLINE_LIMIT`, `MAX_STAT_LIMIT`)는 Binance 제약에 맞춰 설정되어 있으며 필요 시 수정 가능합니다.

## 수집되는 파일
각 심볼별로 다음 CSV가 생성됩니다. 첫 행에는 `# collected_at_kst,YYYY-MM-DD HH:MM:SS KST` 형태의 메타 정보가 포함됩니다.

- `{symbol}_klines_{INTERVAL}_{DAYS}d.csv`
- `{symbol}_oi_{OI_PERIOD}_{DAYS}d.csv`
- `{symbol}_funding_{DAYS}d.csv`
- `{symbol}_global_ls_{DAYS}d.csv`
- `{symbol}_top_ls_accounts_{DAYS}d.csv`
- `{symbol}_top_ls_positions_{DAYS}d.csv`
- `{symbol}_taker_vol_{DAYS}d.csv`
- `{symbol}_index_{INTERVAL}_{DAYS}d.csv`
- `{symbol}_mark_{INTERVAL}_{DAYS}d.csv`
- `{symbol}_premium_{INTERVAL}_{DAYS}d.csv`

## 실행 방법
### 로컬 실행
1. Python 3.11 이상과 가상환경을 준비합니다.
2. `pip install -r requirements.txt`
3. `python collector.py`

성공하면 `data/` 디렉터리에 CSV가 생성되고, 로그에 성공/재시도/스킵 정보가 출력됩니다.

### GitHub Actions
워크플로는 기본적으로 15분 간격(`*/15 * * * *`)으로 실행되며, 레포의 Actions 탭에서 `Run workflow` 버튼을 눌러 즉시 실행할 수 있습니다. 수집 후 변경사항이 존재하면 KST 타임스탬프를 포함한 메시지로 커밋/푸시합니다.

## Binance API 제약 및 백오프 정책
- 모든 호출은 `https://fapi.binance.com` 기반의 USDS-M Futures 퍼블릭 REST 엔드포인트를 사용합니다.
- Kline/Index/Mark/Premium API는 요청당 최대 1500개까지 가능하며, 스크립트는 30일(15분 봉 기준 2880개)을 역방향 페이징으로 나눠 수집합니다.
- Open Interest, Long/Short, Taker Volume 관련 엔드포인트는 limit ≤ 500 조건을 지키며 30일 데이터를 페이징합니다.
- HTTP 429/418(레이트리밋/임시 차단) 발생 시 `Retry-After` 헤더를 우선 사용하고, 없다면 5초부터 시작하는 지수 백오프 + 지터로 최대 5회 재시도합니다.
- HTTP 400은 잘못된 파라미터로 간주해 오류를 로그에 남기고 해당 요청을 스킵합니다.

## 제한 사항 및 유의점
- Binance API 정책/제한이 변경되면 스크립트가 실패할 수 있으므로 주기적으로 모니터링하십시오.
- GitHub Actions에서 푸시가 일어나므로 메인 브랜치 보호 규칙을 확인한 뒤 사용하세요.
- 본 프로젝트는 투자 조언이 아니며, 데이터 품질 및 정확성은 Binance API에 의존합니다.
- 대량 호출을 피하기 위해 요청 수를 최소화했지만, 추가적인 백오프나 캐싱이 필요하다면 `collector.py` 내부 로직을 조정하십시오.

## 예제 로그
```
$ python collector.py
2024-05-12 21:15:03 [INFO] Starting collection for symbols=ARBUSDT,BTCUSDT,ETHUSDT interval=15m days=30
...
2024-05-12 21:17:42 [INFO] Saved data/ETHUSDT_premium_15m_30d.csv (2880 rows)
2024-05-12 21:17:42 [INFO] Collection finished. Success=30 skipped=0
```

> 모든 타임스탬프는 Asia/Seoul(KST) 기준입니다.

## 면책 조항
이 저장소는 연구/자동화 운영 목적이며, 어떠한 투자 조언도 제공하지 않습니다.
