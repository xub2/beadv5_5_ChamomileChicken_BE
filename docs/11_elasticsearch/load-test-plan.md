# 상품 검색 부하 테스트 — 측정 플랜

## 배경 및 제약

데브코스 파이널 프로젝트에서 팀 당 클라우드 환경은 AWS t3.large(2 vCPU / 8 GB) 인스턴스 1개였다.
그러나 5개 팀이 하나의 VPC를 공유하는 구조였고, 같은 VPC 내에서 대규모 부하 트래픽을 발생시키면
다른 팀의 서비스에 영향을 줄 수 있어 클라우드 환경에서의 부하 테스트는 진행하지 않기로 팀 내 합의했다.

대신 각자 담당한 기능에 대해 로컬 환경에서 부하 테스트를 진행했다.
로컬 머신과 t3.large 간 스펙 차이를 최대한 좁히기 위해 아래 환경 제약 방법론을 적용했다.

---

## 환경 제약 방법론 — t3.large 로컬 시뮬레이션

### t3.large 스펙

| 항목 | 값 |
|------|----|
| vCPU | 2 |
| RAM | 8 GB |
| 네트워크 | 최대 5 Gbps (버스트) |

### 리소스 제한 적용

**인프라 컨테이너 (docker update)**

```bash
# Elasticsearch: t3.large 기준 ES 권장 힙 최대 512m × 2 = 1GB
docker update --cpus="0.5" --memory="1g"   elasticsearch

# PostgreSQL: DB I/O 경쟁 재현
docker update --cpus="0.5" --memory="1g"   postgres

# Kafka: 브로커 기본 메모리 할당
docker update --cpus="0.3" --memory="512m" kafka
```

**product 서비스 JVM 힙 제한**

```bash
# t3.large에서 7개 서비스 공존 시 서비스당 약 512m~1g 할당 가정
JAVA_OPTS="-Xms512m -Xmx1024m" ./gradlew :service:product:bootRun
```

**ES 힙 — docker-compose.yml 기준**

```yaml
environment:
  - ES_JAVA_OPTS=-Xms512m -Xmx512m  # 이미 적용됨
```

### 환경 제약의 한계와 보완

로컬 머신(Apple Silicon M 시리즈 등)과 x86 기반 t3.large는 CPU 아키텍처가 달라
클럭 당 처리 성능을 동일하게 재현하는 것은 불가능하다.

이 테스트에서 핵심 지표는 **절대 수치(ms)가 아니라 검색 방식 간 상대적 성능 차이와 실패율**이다.
`Full Scan → GIN → ES`로 넘어갈수록 레이턴시가 줄고 실패율이 0%에 수렴하는 경향은
아키텍처가 달라도 동일하게 관찰된다.

---

## 데이터 준비 — 100만 건 시딩

```bash
# 1. PostgreSQL에 제품 데이터 삽입 (약 2~3분 소요)
psql -U postgres -d userdb -f docs/11_elasticsearch/seed-1m.sql

# 2. 삽입 확인
psql -U postgres -d userdb -c "SELECT COUNT(*) FROM products WHERE status = 'ENABLE';"
# → 1000000

# 3. ES 초기 색인 (Phase 3 전에만 실행)
curl -X POST http://localhost:9004/api/v1/products/es-migrate
# → {"indexed": 1000000}
```

---

## 3-Phase 테스트 절차

### Phase 1 — Full Scan (LIKE %keyword%, 인덱스 없음)

```bash
# GIN 인덱스 제거 (초기 상태 확인)
psql -U postgres -d userdb -c "DROP INDEX IF EXISTS idx_products_title_gin;"

# 테스트 실행
TEST_MODE=full_scan k6 run load-test.js
```

### Phase 2 — GIN 인덱스 적용

```bash
# pg_trgm 확장 + GIN 인덱스 생성 (CONCURRENTLY로 서비스 중단 없이 생성 가능)
psql -U postgres -d userdb -c "
  CREATE EXTENSION IF NOT EXISTS pg_trgm;
  CREATE INDEX CONCURRENTLY idx_products_title_gin
    ON products USING gin (title gin_trgm_ops);"

# 인덱스 생성 확인
psql -U postgres -d userdb -c "\d products"

# 테스트 실행
TEST_MODE=gin k6 run load-test.js
```

### Phase 3 — Elasticsearch

```bash
# ES 초기 색인
curl -X POST http://localhost:9004/api/v1/products/es-migrate

# 색인 건수 확인
curl "http://localhost:9200/products/_count" | python3 -m json.tool

# 테스트 실행
TEST_MODE=es k6 run load-test.js
```

---

## 테스트 구성

| 항목 | 값 |
|------|----|
| 도구 | k6 |
| 데이터 | 100만 건 (products 테이블 + ES 인덱스) |
| VU (Virtual User) | 최대 100 |
| 부하 유지 시간 | 3분 (max 100 VU 구간) |
| 검색 키워드 | 카리나, 윈터, 닝닝, 지젤, 클래스, 공방, 요리, 드로잉 (랜덤) |
| 페이지 | `thisPage=0&pageSize=10` |
| Think time | 0.5초 |

**k6 stages:**

```
30s → 10 VU  (워밍업)
1m  → 100 VU (부하 증가)
3m  → 100 VU (최대 부하 유지)
30s → 0 VU   (종료)
```

---

## 측정 결과

| Phase | 방식 | p95 레이턴시 | 실패 건수 | 비고 |
|-------|------|-------------|----------|------|
| 1 | Full Scan (LIKE %keyword%) | 1,879 ms | 0건 | GIN 인덱스 없음 |
| 2 | GIN 인덱스 (`pg_trgm`) | 1,654 ms | 14건 | 극한 부하 시 실패 발생 |
| 3 | Elasticsearch (nori) | 1,294 ms | 0건 | 역인덱스 + 형태소 분석 |

**GIN 대비 ES:**
- p95 레이턴시: 1,654ms → 1,294ms, **22% 개선**
- Full Scan 대비: 1,879ms → 1,294ms, **31% 개선**
- GIN에서 발생한 14건 실패: ES에서 **0건으로 제거**

---

## 결과 해석

**GIN 실패 원인**

GIN 인덱스 적용 후 평균 레이턴시는 줄었지만, 극한 부하(100 VU 동시)에서 일부 요청이 실패했다.
JPA `LIKE %keyword%`는 키워드 양쪽에 와일드카드가 붙어 GIN 인덱스를 타지 못하는 케이스가 존재한다.
`pg_trgm`의 `%` 연산자(`similarity`)는 trigram 기반이라 `LIKE %keyword%`와 작동 방식이 달라,
Spring Data JPA의 `TitleContaining`(→ `LIKE %keyword%`) 쿼리에서 GIN 이점을 100% 못 살린다.

**ES가 실패 없는 이유**

ES는 역인덱스 구조로 읽기 요청을 메모리 내 검색으로 처리한다.
DB처럼 row-level 락 경쟁이 없고, nori 분석 후 캐시된 토큰 단위로 빠르게 매칭된다.
100 VU 동시 요청에서도 DB 연결 풀 고갈 없이 안정적으로 응답했다.

**DB fallback 레이턴시 차이**

ES 기반 `searchAll()`은 sellerName을 ES에 비정규화해서 user 서비스 REST 호출이 없다.
반면 DB `searchByDb()`는 결과 페이지마다 user 서비스에 sellerName 조회 호출이 발생한다.
이 추가 HTTP 레이턴시도 DB 방식이 ES보다 느린 원인 중 하나다.
