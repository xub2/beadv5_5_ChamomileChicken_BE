// =====================================================
// ES 서킷 브레이커 카오스 테스트
//
// 실행 절차:
//   1. k6 run chaos-test.js
//   2. ~1분 후 (NORMAL 구간 끝) ES 강제 종료:
//        docker stop elasticsearch
//        또는: kill $(lsof -ti:9200)
//   3. 에러 스파이크 → CB Open → fallback 전환 관찰
//   4. ~3분 후 (CHAOS 구간 중) ES 재시작:
//        docker start elasticsearch
//   5. 30초 후 CB Half-Open → 자동 복구 관찰
//
// 측정 지표:
//   - search_success_rate: ES 종료 중에도 CB fallback으로 가용성 유지 확인
//   - search_duration: fallback 전환 전후 응답시간 변화
//   - cb_error_count: CB 감지 전 에러 건수 (최대 ~5건 예상)
// =====================================================

import http from 'k6/http';
import { sleep, check } from 'k6';
import { Trend, Rate, Counter } from 'k6/metrics';

const searchDuration = new Trend('search_duration');
const searchSuccessRate = new Rate('search_success_rate');
const cbErrorCount = new Counter('cb_error_count');

const BASE_URL = 'http://localhost:9004';
const KEYWORDS = ['카리나', '윈터', '닝닝', '지젤'];

export const options = {
  stages: [
    { duration: '1m',  target: 50 },  // [NORMAL]  정상 상태 — ES 정상 동작 확인
    { duration: '3m',  target: 50 },  // [CHAOS]   이 구간 시작 시 ES 종료 → CB 감지 → fallback
    { duration: '2m',  target: 50 },  // [RECOVER] 이 구간 시작 전 ES 재시작 → CB 자동 복구
    { duration: '30s', target: 0  },  // [RAMPDOWN]
  ],
  thresholds: {
    // CB fallback 덕분에 ES 장애 중에도 95% 이상 성공 유지 기대
    search_success_rate: ['rate>0.95'],
  },
};

export default function () {
  const keyword = KEYWORDS[Math.floor(Math.random() * KEYWORDS.length)];
  const params = `title=${encodeURIComponent(keyword)}&thisPage=0&pageSize=10&status=ENABLE`;

  const res = http.get(`${BASE_URL}/api/v1/products?${params}`);

  searchDuration.add(res.timings.duration);

  const success = check(res, {
    'status 200': (r) => r.status === 200,
  });

  searchSuccessRate.add(success);

  if (!success) {
    cbErrorCount.add(1);
    console.log(`[ERROR] status=${res.status} duration=${res.timings.duration}ms`);
  }

  sleep(1);
}
