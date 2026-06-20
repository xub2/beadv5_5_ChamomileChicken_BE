INSERT INTO products (
    id,
    seller_id,
    title,
    max_capacity,
    description,
    price,
    status,
    road_address,
    detail_address,
    zonecode,
    latitude,
    longitude,
    reg_dt,
    modify_dt,
    description_path
)
SELECT
    gen_random_uuid(),
    'b1014af0-4f49-4cfa-881c-a1365df2bbb2',
    CASE MOD(X, 20)
        WHEN 0  THEN '카리나의 댄스 클래스 ' || X
        WHEN 1  THEN '카리나의 보컬 클래스 ' || X
        WHEN 2  THEN '카리나의 뷰티 클래스 ' || X
        WHEN 3  THEN '카리나의 퍼포먼스 클래스 ' || X
        WHEN 4  THEN '카리나의 패션 클래스 ' || X
        WHEN 5  THEN '윈터의 댄스 클래스 ' || X
        WHEN 6  THEN '윈터의 보컬 클래스 ' || X
        WHEN 7  THEN '윈터의 피아노 클래스 ' || X
        WHEN 8  THEN '윈터의 뷰티 클래스 ' || X
        WHEN 9  THEN '윈터의 패션 클래스 ' || X
        WHEN 10 THEN '닝닝의 댄스 클래스 ' || X
        WHEN 11 THEN '닝닝의 보컬 클래스 ' || X
        WHEN 12 THEN '닝닝의 요리 클래스 ' || X
        WHEN 13 THEN '닝닝의 뷰티 클래스 ' || X
        WHEN 14 THEN '닝닝의 퍼포먼스 클래스 ' || X
        WHEN 15 THEN '지젤의 댄스 클래스 ' || X
        WHEN 16 THEN '지젤의 보컬 클래스 ' || X
        WHEN 17 THEN '지젤의 드로잉 클래스 ' || X
        WHEN 18 THEN '지젤의 뷰티 클래스 ' || X
        ELSE          '지젤의 퍼포먼스 클래스 ' || X
    END,
    10,
    CASE MOD(X, 20)
        WHEN 0  THEN '에스파 카리나와 함께하는 댄스 퍼포먼스 클래스입니다.'
        WHEN 1  THEN '에스파 카리나의 보컬 트레이닝 클래스입니다.'
        WHEN 2  THEN '에스파 카리나의 K-뷰티 메이크업 클래스입니다.'
        WHEN 3  THEN '에스파 카리나의 무대 퍼포먼스 클래스입니다.'
        WHEN 4  THEN '에스파 카리나와 함께하는 패션 스타일링 클래스입니다.'
        WHEN 5  THEN '에스파 윈터와 함께하는 댄스 퍼포먼스 클래스입니다.'
        WHEN 6  THEN '에스파 윈터의 보컬 트레이닝 클래스입니다.'
        WHEN 7  THEN '에스파 윈터의 피아노 연주 클래스입니다.'
        WHEN 8  THEN '에스파 윈터의 K-뷰티 메이크업 클래스입니다.'
        WHEN 9  THEN '에스파 윈터와 함께하는 패션 스타일링 클래스입니다.'
        WHEN 10 THEN '에스파 닝닝과 함께하는 댄스 퍼포먼스 클래스입니다.'
        WHEN 11 THEN '에스파 닝닝의 보컬 트레이닝 클래스입니다.'
        WHEN 12 THEN '에스파 닝닝의 중식 요리 클래스입니다.'
        WHEN 13 THEN '에스파 닝닝의 K-뷰티 메이크업 클래스입니다.'
        WHEN 14 THEN '에스파 닝닝의 무대 퍼포먼스 클래스입니다.'
        WHEN 15 THEN '에스파 지젤과 함께하는 댄스 퍼포먼스 클래스입니다.'
        WHEN 16 THEN '에스파 지젤의 보컬 트레이닝 클래스입니다.'
        WHEN 17 THEN '에스파 지젤의 일러스트 드로잉 클래스입니다.'
        WHEN 18 THEN '에스파 지젤의 K-뷰티 메이크업 클래스입니다.'
        ELSE          '에스파 지젤의 무대 퍼포먼스 클래스입니다.'
    END,
    10000 + (X * 100),
    'ENABLE',
    '서울 강남구 테헤란로 ' || X,
    '테스트 상세주소',
    '06236',
    37.5000000 + MOD(X, 100) * 0.001,
    127.0000000 + MOD(X, 100) * 0.001,
    NOW(),
    NOW(),
    '[]'
FROM generate_series(1, 1000000) AS X;