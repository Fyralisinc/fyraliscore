-- migration:no-transaction
-- 0158_prune_empty_out_of_window_partitions.sql
--
-- Keep partitioning powerful without letting dev/test/backfill self-heal create
-- unbounded catalog surface. This drops only EMPTY monthly partitions outside a
-- generous window:
--   * keep 36 months behind the current month
--   * keep 6 months ahead
--
-- Non-empty partitions are retained even when old/future, so historical data is
-- not discarded by this simplification pass.
--
-- This migration intentionally runs outside the migration runner's transaction
-- wrapper and prunes in small transactions to avoid max_locks_per_transaction
-- pressure on DBs that have accumulated many empty partitions.

CREATE OR REPLACE PROCEDURE _prune_empty_out_of_window_partitions(batch_size integer)
LANGUAGE plpgsql
AS $$
DECLARE
  r RECORD;
  month_start DATE;
  keep_start DATE := (date_trunc('month', CURRENT_DATE)::date - INTERVAL '36 months')::date;
  keep_end DATE := (date_trunc('month', CURRENT_DATE)::date + INTERVAL '7 months')::date;
  row_count BIGINT;
  dropped_count INTEGER := 0;
BEGIN
  FOR r IN
    SELECT
      child.relname AS partition_name,
      parent.relname AS parent_name,
      substring(child.relname FROM '_(\d{4}_\d{2})$') AS month_key
    FROM pg_inherits inh
    JOIN pg_class child ON child.oid = inh.inhrelid
    JOIN pg_class parent ON parent.oid = inh.inhparent
    JOIN pg_namespace n ON n.oid = parent.relnamespace
    WHERE n.nspname = 'public'
      AND parent.relname IN ('observations', 'resource_transactions')
      AND child.relkind = 'r'
      AND child.relname ~ '_(\d{4}_\d{2})$'
    ORDER BY child.relname
  LOOP
    month_start := to_date(r.month_key, 'YYYY_MM');
    IF month_start >= keep_start AND month_start < keep_end THEN
      CONTINUE;
    END IF;

    EXECUTE format('SELECT count(*) FROM %I', r.partition_name) INTO row_count;
    IF row_count = 0 THEN
      EXECUTE format('DROP TABLE IF EXISTS %I', r.partition_name);
      dropped_count := dropped_count + 1;
      IF dropped_count >= batch_size THEN
        EXIT;
      END IF;
    END IF;
  END LOOP;
END;
$$;

BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;
BEGIN; CALL _prune_empty_out_of_window_partitions(25); COMMIT;

DROP PROCEDURE IF EXISTS _prune_empty_out_of_window_partitions(integer);
