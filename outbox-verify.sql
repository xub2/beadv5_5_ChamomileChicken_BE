-- ============================================================
-- outbox-verify.sql
-- Outbox 패턴 / 보상 트랜잭션 검증 쿼리
-- 실행: psql -h localhost -U postgres -d userdb -f outbox-verify.sql
-- ============================================================

-- ---- 1. 이벤트 유실률: PENDING 잔여 건수 ----
-- PENDING/SENDING이 0건이면 이벤트 유실률 0%
SELECT
    status,
    COUNT(*) AS count
FROM order_outbox_events
GROUP BY status
ORDER BY status;

-- ---- 2. 보상 이벤트 전달률 ----
SELECT
    event_type,
    COUNT(*)                                                             AS total,
    SUM(CASE WHEN status = 'PUBLISHED' THEN 1 ELSE 0 END)              AS published,
    ROUND(
        SUM(CASE WHEN status = 'PUBLISHED' THEN 1 ELSE 0 END)::numeric
        / NULLIF(COUNT(*), 0) * 100, 1
    )                                                                    AS success_rate_pct
FROM order_outbox_events
WHERE event_type IN (
    'ORDER_RESERVATION_RELEASED',    -- 주문 생성 실패 시 재고 복구
    'ORDER_DEPOSIT_REFUND_REQUESTED' -- 결제 실패/환불 시 예치금 복구
)
GROUP BY event_type
ORDER BY event_type;

-- ---- 3. DLQ 격리 건수 확인 (Kafka CLI) ----
-- 아래 명령으로 DLQ 토픽 메시지 수 확인:
--   kcat -b localhost:9092 -t payment.events.dlq -C -e -q -o beginning | wc -l
--   kcat -b localhost:9092 -t order.events.dlq   -C -e -q -o beginning | wc -l

-- ---- 4. 멱등성: processed_events 중복 방어 현황 ----
SELECT
    COUNT(*)              AS total_deduplicated_events,
    MIN(processed_at)     AS first_processed,
    MAX(processed_at)     AS last_processed
FROM order_processed_events;

-- ---- 5. FOR UPDATE SKIP LOCKED 동작 확인 ----
-- 아래 쿼리를 두 psql 세션에서 동시 실행하여 겹치는 id가 없는지 확인
-- (outbox-chaos-test.sh 시나리오 3이 자동으로 검증함)
SELECT id, event_type, status, created_at
FROM order_outbox_events
WHERE status = 'PENDING'
ORDER BY created_at
LIMIT 25
FOR UPDATE SKIP LOCKED;

-- ---- 종합 요약 ----
SELECT
    (SELECT COUNT(*) FROM order_outbox_events WHERE status IN ('PENDING','SENDING'))
        AS pending_events,         -- 0이면 이벤트 유실 없음
    (SELECT COUNT(*) FROM order_outbox_events WHERE status = 'FAILED')
        AS failed_events,          -- 5회 재시도 초과 → DLQ 대상
    (SELECT COUNT(*) FROM order_outbox_events WHERE status = 'PUBLISHED')
        AS published_events,
    (SELECT COUNT(*) FROM order_processed_events)
        AS deduplicated_events,    -- 중복 처리 방어된 이벤트 수
    (SELECT ROUND(
        SUM(CASE WHEN status = 'PUBLISHED' THEN 1 ELSE 0 END)::numeric
        / NULLIF(COUNT(*), 0) * 100, 1
     ) FROM order_outbox_events
     WHERE event_type IN ('ORDER_RESERVATION_RELEASED','ORDER_DEPOSIT_REFUND_REQUESTED'))
        AS compensation_delivery_rate_pct;