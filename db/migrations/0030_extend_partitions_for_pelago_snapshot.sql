-- Extend observations + resource_transactions partitions to cover
-- the full Pelago snapshot window. The snapshot dates events from
-- 2026-01-01 through 2027-01-31; the original 0001 migration only
-- creates partitions for current_month..+3, which doesn't reach
-- Sep–Dec 2026 or Jan 2027.
--
-- Idempotent: CREATE TABLE IF NOT EXISTS PARTITION OF re-runs cleanly.

BEGIN;

DO $$
DECLARE
    start_date DATE := DATE '2026-01-01';
    end_window DATE := DATE '2027-02-01';
    end_date DATE;
    partition_name TEXT;
BEGIN
    WHILE start_date < end_window LOOP
        end_date := (start_date + INTERVAL '1 month')::DATE;

        partition_name := format('observations_%s', TO_CHAR(start_date, 'YYYY_MM'));
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF observations FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );

        partition_name := format('resource_transactions_%s', TO_CHAR(start_date, 'YYYY_MM'));
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I PARTITION OF resource_transactions FOR VALUES FROM (%L) TO (%L)',
            partition_name, start_date, end_date
        );

        start_date := end_date;
    END LOOP;
END $$;

COMMIT;
