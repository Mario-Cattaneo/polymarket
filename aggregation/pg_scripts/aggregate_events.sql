DO $$
DECLARE
    -- Loop Variable
    cfg RECORD;
    
    -- Hard Fork Logic Variables
    table_suffix TEXT;
    current_source_table TEXT;
    current_target_table TEXT;
    table_exists BOOLEAN;
    
    -- Inner Loop Variables
    p_name TEXT;
    p_bound_start BIGINT;
    p_index BIGINT;
    max_existing_index BIGINT;
    is_last_partition BOOLEAN;
    
    -- Dynamic SQL Variables
    insert_stmt TEXT;
    rows_inserted BIGINT;
    start_ts TIMESTAMP;
    
    -- Constants
    ANCHOR_TIME CONSTANT BIGINT := 1763461108242;
    PARTITION_SIZE CONSTANT BIGINT := 10800000;
BEGIN
    -- ==========================================================================================
    -- CONFIGURATION LOOP
    -- ==========================================================================================
    FOR cfg IN SELECT * FROM (VALUES 
        -- 1. buffer_price_changes
        -- FIX: Replaced 'side' with 'message->>''side''' to handle V1 (no side col) and V2 (side col)
        ('buffer_price_changes', 'agg_price_changes', TRUE, 
         'server_time_ms, asset_id, price, message->>''side''', 
         'server_time_ms, asset_id, price, size, message->>''side'', message::text'),
         
        -- 2. buffer_books (No side column involved here usually)
        ('buffer_books', 'agg_books', TRUE, 
         'server_time_ms, asset_id', 
         'server_time_ms, asset_id, message::text'),

        -- 3. buffer_tick_changes
        ('buffer_tick_changes', 'agg_tick_changes', TRUE, 
         'server_time_ms, asset_id', 
         'server_time_ms, asset_id, tick_size, message::text'),
         
        -- 4. buffer_last_trade_prices
        -- FIX: Replaced 'side' with 'message->>''side''' here as well just in case
        ('buffer_last_trade_prices', 'agg_last_trade_prices', FALSE, 
         NULL::text, 
         'server_time_ms, asset_id, price, size, message->>''side'', fee_rate_bps, message::text')
    ) AS t(source_table, target_table, has_ambiguous, ambiguous_cols, duplicate_cols)
    LOOP
        
        -- ======================================================================================
        -- HARD FORK LOOP: Process ('') then ('_2')
        -- ======================================================================================
        FOREACH table_suffix IN ARRAY ARRAY['', '_2']
        LOOP
            -- 1. Construct Names dynamically
            current_source_table := cfg.source_table || table_suffix;
            current_target_table := cfg.target_table || table_suffix;

            RAISE NOTICE '----------------------------------------------------------------';
            RAISE NOTICE 'Processing Pair: % -> %', current_source_table, current_target_table;

            -- 2. Check if Source Table exists (Safety check)
            SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = current_source_table) INTO table_exists;
            IF NOT table_exists THEN
                RAISE NOTICE 'Source table % does not exist. Skipping.', current_source_table;
                CONTINUE;
            END IF;

            -- 3. Check Resume Point
            SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = current_target_table) INTO table_exists;
            IF NOT table_exists THEN
                RAISE NOTICE 'Target table % does not exist. Please create it first.', current_target_table;
                CONTINUE; 
            END IF;

            EXECUTE format('SELECT MAX(partition_index) FROM %I', current_target_table) INTO max_existing_index;
            
            IF max_existing_index IS NULL THEN 
                max_existing_index := -1; 
                RAISE NOTICE 'Target % is empty. Starting from beginning.', current_target_table;
            ELSE
                RAISE NOTICE 'Target % resuming from partition index: %', current_target_table, max_existing_index;
            END IF;

            -- 4. Iterate Partitions
            FOR p_name, p_bound_start IN 
                SELECT
                    child.relname,
                    (regexp_match(pg_get_expr(child.relpartbound, child.oid), 'FROM \(''(\d+)''\)'))[1]::bigint
                FROM pg_inherits
                JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
                JOIN pg_class child ON pg_inherits.inhrelid = child.oid
                WHERE parent.relname = current_source_table
                ORDER BY 2 ASC
            LOOP
                p_index := FLOOR((p_bound_start - ANCHOR_TIME) / PARTITION_SIZE);

                -- 5. Active Partition Logic
                IF table_suffix = '_2' THEN
                    -- New table: Skip active partition
                    SELECT NOT EXISTS (
                        SELECT 1 FROM pg_inherits i JOIN pg_class c ON i.inhrelid = c.oid
                        WHERE i.inhparent = current_source_table::regclass
                        AND (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'FROM \(''(\d+)''\)'))[1]::bigint > p_bound_start
                    ) INTO is_last_partition;

                    IF is_last_partition THEN
                        RAISE NOTICE 'Reached active partition in % (Index %). Stopping.', current_source_table, p_index;
                        EXIT; 
                    END IF;
                ELSE
                    -- Old table: Process everything
                    is_last_partition := FALSE;
                END IF;

                -- 6. Skip processed
                IF p_index <= max_existing_index THEN
                    CONTINUE;
                END IF;

                -- 7. Process
                start_ts := clock_timestamp();
                RAISE NOTICE 'Aggregating % (Index %)...', p_name, p_index;

                -- Cleanup
                EXECUTE format('DELETE FROM %I WHERE partition_index = $1', current_target_table) USING p_index;

                -- Insert
                IF cfg.has_ambiguous THEN
                    insert_stmt := format('
                        INSERT INTO %I (partition_index, partition_start_time_ms, asset_id, total_count, ambiguous_count, duplicate_count)
                        SELECT 
                            FLOOR((found_time_ms - %s) / %s) AS partition_index,
                            %s + (FLOOR((found_time_ms - %s) / %s) * %s) AS partition_start_time_ms,
                            asset_id,
                            COUNT(*) AS total_count,
                            COUNT(*) - COUNT(DISTINCT (%s)) AS ambiguous_count,
                            COUNT(*) - COUNT(DISTINCT (%s)) AS duplicate_count
                        FROM %I 
                        GROUP BY 1, 2, 3',
                        current_target_table, 
                        ANCHOR_TIME, PARTITION_SIZE, 
                        ANCHOR_TIME, ANCHOR_TIME, PARTITION_SIZE, PARTITION_SIZE,
                        cfg.ambiguous_cols, 
                        cfg.duplicate_cols, 
                        p_name
                    );
                ELSE
                    insert_stmt := format('
                        INSERT INTO %I (partition_index, partition_start_time_ms, asset_id, total_count, duplicate_count)
                        SELECT 
                            FLOOR((found_time_ms - %s) / %s) AS partition_index,
                            %s + (FLOOR((found_time_ms - %s) / %s) * %s) AS partition_start_time_ms,
                            asset_id,
                            COUNT(*) AS total_count,
                            COUNT(*) - COUNT(DISTINCT (%s)) AS duplicate_count
                        FROM %I 
                        GROUP BY 1, 2, 3',
                        current_target_table, 
                        ANCHOR_TIME, PARTITION_SIZE, 
                        ANCHOR_TIME, ANCHOR_TIME, PARTITION_SIZE, PARTITION_SIZE,
                        cfg.duplicate_cols, 
                        p_name
                    );
                END IF;

                EXECUTE insert_stmt;
                GET DIAGNOSTICS rows_inserted = ROW_COUNT;
                RAISE NOTICE 'Saved to %: % rows in %', current_target_table, rows_inserted, (clock_timestamp() - start_ts);

            END LOOP; -- End Partition Loop
        END LOOP; -- End Suffix Loop
    END LOOP; -- End Config Loop
    
    RAISE NOTICE 'All tables processed successfully.';
END $$;