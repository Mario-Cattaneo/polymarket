-- ============================================================================
-- 1. CLEANUP (Drop incorrect tables)
-- ============================================================================
DROP TABLE IF EXISTS buffer_miss_rows CASCADE;
DROP TABLE IF EXISTS buffer_miss_rows_2 CASCADE;
DROP TABLE IF EXISTS agg_partition_counts CASCADE;
DROP TABLE IF EXISTS agg_partition_counts_2 CASCADE;
DROP TABLE IF EXISTS miss_count_asset_agg CASCADE;

-- ============================================================================
-- 2. RAW MISS ROWS (Must be PARTITIONED BY RANGE)
-- ============================================================================

-- V1: Partitioned Parent
CREATE TABLE buffer_miss_rows (
    LIKE buffer_server_book INCLUDING ALL
) PARTITION BY RANGE (found_time_ms);

-- V2: Partitioned Parent
CREATE TABLE buffer_miss_rows_2 (
    LIKE buffer_server_book_2 INCLUDING ALL
) PARTITION BY RANGE (found_time_ms);

-- ============================================================================
-- 3. SIMPLE PARTITION COUNTS (Standard Tables)
-- ============================================================================
CREATE TABLE agg_partition_counts (
    partition_index BIGINT NOT NULL,
    partition_start_time_ms BIGINT NOT NULL,
    asset_id TEXT NOT NULL,
    total_count BIGINT NOT NULL,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_agg_counts_part ON agg_partition_counts(partition_index);

CREATE TABLE agg_partition_counts_2 (
    LIKE agg_partition_counts INCLUDING ALL
);

-- ============================================================================
-- 4. DETAILED MISS AGGREGATION (V1 Only)
-- ============================================================================
CREATE TABLE miss_count_asset_agg (
    partition_index BIGINT NOT NULL,
    partition_start_time_ms BIGINT NOT NULL,
    asset_id TEXT NOT NULL,
    total_count BIGINT,
    count_miss_0 BIGINT,
    count_miss_1 BIGINT,
    count_miss_2 BIGINT,
    count_miss_3 BIGINT,
    count_miss_4 BIGINT,
    count_miss_5 BIGINT,
    count_miss_6 BIGINT,
    count_miss_7 BIGINT,
    count_miss_8 BIGINT,
    count_miss_9 BIGINT,
    count_miss_10 BIGINT,
    count_miss_11 BIGINT,
    count_miss_12 BIGINT,
    count_miss_13 BIGINT,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_miss_agg_part ON miss_count_asset_agg(partition_index);