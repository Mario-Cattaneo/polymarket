-- 1. Tables with 'ambiguous_count' (Price, Books, Ticks)
CREATE TABLE IF NOT EXISTS agg_price_changes (
    partition_index BIGINT,
    partition_start_time_ms BIGINT,
    asset_id TEXT,  -- Change to INTEGER if your asset_ids are numbers
    total_count BIGINT,
    ambiguous_count BIGINT,
    duplicate_count BIGINT
);

CREATE TABLE IF NOT EXISTS agg_books (
    partition_index BIGINT,
    partition_start_time_ms BIGINT,
    asset_id TEXT,
    total_count BIGINT,
    ambiguous_count BIGINT,
    duplicate_count BIGINT
);

CREATE TABLE IF NOT EXISTS agg_tick_changes (
    partition_index BIGINT,
    partition_start_time_ms BIGINT,
    asset_id TEXT,
    total_count BIGINT,
    ambiguous_count BIGINT,
    duplicate_count BIGINT
);

-- 2. Table WITHOUT 'ambiguous_count' (Last Trades)
CREATE TABLE IF NOT EXISTS agg_last_trade_prices (
    partition_index BIGINT,
    partition_start_time_ms BIGINT,
    asset_id TEXT,
    total_count BIGINT,
    duplicate_count BIGINT
);

-- Optional: Add indexes for performance (since the script queries MAX(partition_index) and DELETEs by it)
CREATE INDEX IF NOT EXISTS idx_agg_price_pid ON agg_price_changes(partition_index);
CREATE INDEX IF NOT EXISTS idx_agg_books_pid ON agg_books(partition_index);
CREATE INDEX IF NOT EXISTS idx_agg_tick_pid ON agg_tick_changes(partition_index);
CREATE INDEX IF NOT EXISTS idx_agg_trade_pid ON agg_last_trade_prices(partition_index);
-- 1. Create the _2 tables (Copies structure of original agg tables)
CREATE TABLE IF NOT EXISTS agg_price_changes_2 (LIKE agg_price_changes INCLUDING ALL);
CREATE TABLE IF NOT EXISTS agg_books_2 (LIKE agg_books INCLUDING ALL);
CREATE TABLE IF NOT EXISTS agg_tick_changes_2 (LIKE agg_tick_changes INCLUDING ALL);
CREATE TABLE IF NOT EXISTS agg_last_trade_prices_2 (LIKE agg_last_trade_prices INCLUDING ALL);

-- 2. Ensure Indexes exist on the new tables (LIKE ... INCLUDING ALL usually handles this, but good to verify)
CREATE INDEX IF NOT EXISTS idx_agg_price_2_pid ON agg_price_changes_2(partition_index);
CREATE INDEX IF NOT EXISTS idx_agg_books_2_pid ON agg_books_2(partition_index);
CREATE INDEX IF NOT EXISTS idx_agg_tick_2_pid ON agg_tick_changes_2(partition_index);
CREATE INDEX IF NOT EXISTS idx_agg_trade_2_pid ON agg_last_trade_prices_2(partition_index);