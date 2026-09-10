BEGIN;

INSERT INTO markets
SELECT
    format('PLAT-%s', lpad(i::text, 3, '0')),
    format('CyberMarket %s', i),
    CASE WHEN i % 3 = 0 THEN 'marketplace' ELSE 'specialized' END,
    365 + i * 30,
    'active',
    (70 + i % 30)::text,
    CASE WHEN i % 4 = 0 THEN 'medium' ELSE 'high' END,
    CASE WHEN i < 4 THEN 'large' ELSE 'medium' END,
    (1000 + i * 125)::real,
    (10000 + i * 900)::text,
    1000,
    10000,
    4000,
    clock_timestamp(),
    24,
    jsonb_build_object('status', 'compliant', 'review_cycle_days', 90)
FROM generate_series(1, 10) AS g(i)
ON CONFLICT DO NOTHING;

INSERT INTO vendors
SELECT
    format('SELLER-%s', lpad(i::text, 6, '0')),
    30 + i % 2000,
    (3.0 + (i % 20) / 10.0)::real,
    (i % 5000)::text,
    i % 4900,
    i % 20,
    CASE WHEN i % 10 = 0 THEN 'enhanced' ELSE 'standard' END,
    clock_timestamp() - make_interval(days => i % 30),
    'active',
    CASE WHEN i % 97 = 0 THEN 'review' ELSE 'none' END,
    CASE WHEN i % 53 = 0 THEN 'medium' ELSE 'low' END,
    CASE WHEN i % 41 = 0 THEN 'medium' ELSE 'low' END,
    'verified',
    jsonb_build_object('score', 60 + i % 40, 'reviewed', true)
FROM generate_series(1, 1000) AS g(i)
ON CONFLICT DO NOTHING;

INSERT INTO buyers
SELECT
    format('BUYER-%s', lpad(i::text, 7, '0')),
    i % 2500,
    i % 100,
    CASE WHEN i % 3 = 0 THEN 'mfa' ELSE 'standard' END,
    jsonb_build_object('score', (i % 1000) / 1000.0, 'segment', i % 20)
FROM generate_series(1, 10000) AS g(i)
ON CONFLICT DO NOTHING;

INSERT INTO products
SELECT
    format('CAT-%s', lpad((i % 20)::text, 3, '0')),
    format('SUB-%s', lpad((i % 100)::text, 5, '0')),
    (i % 4)::bigint,
    format('SELLER-%s', lpad((1 + (i - 1) / 4)::text, 6, '0')),
    jsonb_build_object('stock', 50 + i % 450, 'available', true)
FROM generate_series(1, 4000) AS g(i)
ON CONFLICT DO NOTHING;

COMMIT;
