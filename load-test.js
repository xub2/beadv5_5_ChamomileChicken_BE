// 실행 방법:
//   Phase 1 (GIN 인덱스 없음): k6 run load-test.js
//   Phase 2 (GIN 인덱스 적용 후): k6 run load-test.js
//   두 결과를 비교해 ES / DB(풀스캔) / DB(GIN) 3방향 성능 비교

import http from 'k6/http';
import { sleep } from 'k6';
import { Trend, Rate } from 'k6/metrics';

const esDuration = new Trend('es_duration');
const dbDuration = new Trend('db_duration');
const esSuccess = new Rate('es_success');
const dbSuccess = new Rate('db_success');

const BASE_URL = 'http://localhost:9004';
const KEYWORDS = ['카리나', '윈터', '닝닝', '지젤'];

export const options = {
  stages: [
    { duration: '30s', target: 10  },
    { duration: '1m',  target: 100 },
    { duration: '3m',  target: 100 },
    { duration: '30s', target: 0   },
  ],
  thresholds: {
    es_duration: ['p(95)<500'],
    db_duration: ['p(95)<5000'],
  },
};

export default function () {
  const keyword = KEYWORDS[Math.floor(Math.random() * KEYWORDS.length)];
  const params = `title=${encodeURIComponent(keyword)}&thisPage=0&pageSize=10&status=ENABLE`;

  const esRes = http.get(`${BASE_URL}/api/v1/products?${params}`);
  esDuration.add(esRes.timings.duration);
  esSuccess.add(esRes.status === 200);

  sleep(0.5);

  const dbRes = http.get(`${BASE_URL}/api/v1/products/search-db?${params}`);
  dbDuration.add(dbRes.timings.duration);
  dbSuccess.add(dbRes.status === 200);

  sleep(0.5);
}