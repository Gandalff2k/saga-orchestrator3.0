// k6 load test for POST /orders.
//
//   docker run --rm -i -e BASE_URL=http://host.docker.internal:8080 -e RATE=50 -e DURATION=60s \
//     -e SEAT_OFFSET=10000 grafana/k6:2.3.0 run - < test/load/orders.js
//
// It measures the API side (accept latency, error rate). End-to-end saga
// latency is measured in the database afterwards (sagas.updated_at -
// created_at), which is exact and doesn't double the load with polling.
import http from 'k6/http';
import { check } from 'k6';
import exec from 'k6/execution';

const BASE = __ENV.BASE_URL || 'http://localhost:8080';
const OFFSET = parseInt(__ENV.SEAT_OFFSET || '10000', 10);

export const options = {
  scenarios: {
    orders: {
      // Arrival rate, not VUs: the API is asked for RATE orders/s whatever its
      // latency, which is how real traffic behaves (open model).
      executor: 'constant-arrival-rate',
      rate: parseInt(__ENV.RATE || '50', 10),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 50,
      maxVUs: 200,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.001'],
    http_req_duration: ['p(95)<250'],
  },
};

export default function () {
  // A distinct seat per iteration: every order can succeed, so failures in
  // the results mean the system failed, not the test data.
  const seat = OFFSET + exec.scenario.iterationInTest;
  const res = http.post(`${BASE}/orders`, JSON.stringify({ seat_id: seat, amount_cents: 1000 }), {
    headers: { 'Content-Type': 'application/json', 'Idempotency-Key': `load-${OFFSET}-${seat}` },
  });
  check(res, { 'accepted (202)': (r) => r.status === 202 });
}
