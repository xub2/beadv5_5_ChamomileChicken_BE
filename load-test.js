// =====================================================
// 상품 검색 3-Phase 부하 테스트 — k6
// Full Scan (DB LIKE) / GIN 인덱스 / Elasticsearch 성능 비교
//
// ── 실행 순서 ─────────────────────────────────────────
//   Phase 1 (Full Scan — GIN 없음):
//     psql -U postgres -d userdb -c "DROP INDEX IF EXISTS idx_products_title_gin;"
//     TEST_MODE=full_scan k6 run load-test.js
//
//   Phase 2 (GIN 인덱스 적용 후):
//     psql -U postgres -d userdb -c "
//       CREATE EXTENSION IF NOT EXISTS pg_trgm;
//       CREATE INDEX CONCURRENTLY idx_products_title_gin
//         ON products USING gin (title gin_trgm_ops);"
//     TEST_MODE=gin k6 run load-test.js
//
//   Phase 3 (Elasticsearch):
//     curl -X POST http://localhost:9004/api/v1/products/es-migrate
//     TEST_MODE=es k6 run load-test.js
//
// ── t3.large 로컬 시뮬레이션 (2 vCPU / 8 GB) ──────────
//   [인프라 컨테이너 리소스 상한 — docker update로 적용]
//     docker update --cpus="0.5" --memory="1g"   elasticsearch
//     docker update --cpus="0.5" --memory="1g"   postgres
//     docker update --cpus="0.3" --memory="512m" kafka
//
//   [product 서비스 JVM 힙 제한]
//     JAVA_OPTS="-Xms512m -Xmx1024m" ./gradlew :service:product:bootRun
//
//   리소스 제한 목적: 절대 수치보다 검색 방식 간 상대적 차이(레이턴시, 실패율)가
//   핵심 지표이므로, 제한을 통해 t3.large와 유사한 자원 경쟁 압력을 재현한다.
//
// ── 데이터 준비 (100만 건) ────────────────────────────
//   psql -U postgres -d userdb -f docs/11_elasticsearch/seed-1m.sql
// =====================================================

import http from 'k6/http';
import { sleep, check } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';

const searchDuration = new Trend('search_duration');
const successRate    = new Rate('search_success_rate');
const failCount      = new Counter('search_fail_count');

const BASE_URL  = __ENV.BASE_URL  || 'http://localhost:9004';
const TEST_MODE = __ENV.TEST_MODE || 'es'; // full_scan | gin | es

const ENDPOINT = TEST_MODE === 'es'
  ? '/api/v1/products'
  : '/api/v1/products/search-db';

const KEYWORDS = ['카리나', '윈터', '닝닝', '지젤', '클래스', '공방', '요리', '드로잉'];

export const options = {
  stages: [
    { duration: '30s', target: 10  }, // 워밍업
    { duration: '1m',  target: 100 }, // 부하 증가
    { duration: '3m',  target: 100 }, // 최대 부하 유지 (100 VU)
    { duration: '30s', target: 0   }, // 종료
  ],
  thresholds: {
    search_success_rate: ['rate>0.99'],
    search_duration: TEST_MODE === 'es'
      ? ['p(95)<1600', 'p(99)<2500']
      : TEST_MODE === 'gin'
        ? ['p(95)<1900', 'p(99)<3000']
        : ['p(95)<2500', 'p(99)<5000'],
  },
};

export function setup() {
  if (TEST_MODE === 'es') {
    const esRes = http.get('http://localhost:9200/_cluster/health');
    if (esRes.status === 200) {
      const body = JSON.parse(esRes.body);
      console.log(`[ES] cluster=${body.status} nodes=${body.number_of_nodes}`);
    } else {
      console.warn(`[ES] health check failed: status=${esRes.status}`);
    }
  }

  const svcRes = http.get(`${BASE_URL}/api/v1/products?status=ENABLE&thisPage=0&pageSize=1`);
  console.log(`[READY] TEST_MODE=${TEST_MODE} endpoint=${ENDPOINT} service_status=${svcRes.status}`);
}

export default function () {
  const keyword = KEYWORDS[Math.floor(Math.random() * KEYWORDS.length)];
  const params  = `title=${encodeURIComponent(keyword)}&thisPage=0&pageSize=10&status=ENABLE`;
  const res     = http.get(`${BASE_URL}${ENDPOINT}?${params}`);

  searchDuration.add(res.timings.duration);

  const ok = check(res, { 'status 200': (r) => r.status === 200 });
  successRate.add(ok);

  if (!ok) {
    failCount.add(1);
    console.error(`[FAIL] mode=${TEST_MODE} status=${res.status} keyword=${keyword} ${res.timings.duration}ms`);
  }

  sleep(0.5);
}
