DO $$
DECLARE
    -- Configuration
    cfg RECORD;
    
    -- Loop Variables
    p_name TEXT;
    p_bound_start BIGINT;
    p_bound_end BIGINT;
    p_index BIGINT;
    max_existing_index BIGINT;
    is_last_partition BOOLEAN;
    part_exists BOOLEAN; -- New variable for silent check
    
    -- Dynamic SQL Variables
    target_part_name TEXT;
    rows_raw BIGINT;
    rows_simple BIGINT;
    rows_detailed BIGINT;
    start_ts TIMESTAMP;
    
    -- Constants
    ANCHOR_TIME CONSTANT BIGINT := 1763461108242;
    PARTITION_SIZE CONSTANT BIGINT := 10800000; 
BEGIN
    -- ==========================================================================================
    -- CONFIGURATION LOOP
    -- ==========================================================================================
    FOR cfg IN SELECT * FROM (VALUES 
        ('buffer_server_book', 'buffer_miss_rows', 'agg_partition_counts', 'miss_count_asset_agg', FALSE),
        ('buffer_server_book_2', 'buffer_miss_rows_2', 'agg_partition_counts_2', NULL::text, TRUE)
    ) AS t(source_tbl, target_raw, target_simple, target_detailed, skip_active)
    LOOP
        
        RAISE NOTICE '----------------------------------------------------------------';
        RAISE NOTICE 'PROCESSING: %', cfg.source_tbl;

        -- 1. Check Resume Point 
        EXECUTE format('SELECT MAX(partition_index) FROM %I', cfg.target_simple) INTO max_existing_index;
        
        IF max_existing_index IS NULL THEN 
            max_existing_index := -1; 
            RAISE NOTICE 'Target is empty. Starting from beginning.';
        ELSE
            RAISE NOTICE 'Resuming from partition index: %', max_existing_index;
        END IF;

        -- 2. Iterate Partitions
        FOR p_name, p_bound_start, p_bound_end IN 
            SELECT
                child.relname,
                (regexp_match(pg_get_expr(child.relpartbound, child.oid), 'FROM \(''(\d+)''\)'))[1]::bigint,
                (regexp_match(pg_get_expr(child.relpartbound, child.oid), 'TO \(''(\d+)''\)'))[1]::bigint
            FROM pg_inherits
            JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
            JOIN pg_class child ON pg_inherits.inhrelid = child.oid
            WHERE parent.relname = cfg.source_tbl
            ORDER BY 2 ASC
        LOOP
            p_index := FLOOR((p_bound_start - ANCHOR_TIME) / PARTITION_SIZE);

            -- 3. Active Partition Logic
            IF cfg.skip_active THEN
                SELECT NOT EXISTS (
                    SELECT 1 FROM pg_inherits i JOIN pg_class c ON i.inhrelid = c.oid
                    WHERE i.inhparent = cfg.source_tbl::regclass
                    AND (regexp_match(pg_get_expr(c.relpartbound, c.oid), 'FROM \(''(\d+)''\)'))[1]::bigint > p_bound_start
                ) INTO is_last_partition;

                IF is_last_partition THEN
                    RAISE NOTICE 'Reached active partition in % (Index %). Stopping.', cfg.source_tbl, p_index;
                    EXIT; 
                END IF;
            END IF;

            -- 4. Skip processed
            IF p_index < max_existing_index THEN
                CONTINUE;
            END IF;

            -- 5. PROCESS
            start_ts := clock_timestamp();
            RAISE NOTICE 'Processing % (Index %)...', p_name, p_index;

            -- ====================================================
            -- A. TABLE 1: Raw Miss Rows (Partitioned)
            -- ====================================================
            
            target_part_name := cfg.target_raw || '_' || substring(p_name from 'p\d+$');

            -- SILENT DROP: Check existence first to avoid "NOTICE: skipping"
            SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = target_part_name) INTO part_exists;
            IF part_exists THEN
                EXECUTE format('DROP TABLE %I', target_part_name);
            END IF;

            -- Create partition
            EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)', 
                           target_part_name, cfg.target_raw, p_bound_start, p_bound_end);

            -- Insert filtered rows
            EXECUTE format('
                INSERT INTO %I 
                SELECT * FROM %I 
                WHERE false_misses > 0',
                target_part_name, 
                p_name
            );
            GET DIAGNOSTICS rows_raw = ROW_COUNT;

            -- ====================================================
            -- B. TABLE 2: Simple Partition Counts
            -- ====================================================

            EXECUTE format('DELETE FROM %I WHERE partition_index = $1', cfg.target_simple) USING p_index;

            EXECUTE format('
                INSERT INTO %I (partition_index, partition_start_time_ms, asset_id, total_count)
                SELECT 
                    FLOOR((found_time_ms - %s) / %s) AS partition_index,
                    %s + (FLOOR((found_time_ms - %s) / %s) * %s) AS partition_start_time_ms,
                    asset_id,
                    COUNT(*) AS total_count
                FROM %I 
                GROUP BY 1, 2, 3',
                cfg.target_simple,
                ANCHOR_TIME, PARTITION_SIZE, 
                ANCHOR_TIME, ANCHOR_TIME, PARTITION_SIZE, PARTITION_SIZE,
                p_name
            );
            GET DIAGNOSTICS rows_simple = ROW_COUNT;

            -- ====================================================
            -- C. TABLE 3: Detailed Miss Aggregation (V1 Only)
            -- ====================================================
            
            rows_detailed := 0;
            
            IF cfg.target_detailed IS NOT NULL THEN
                EXECUTE format('DELETE FROM %I WHERE partition_index = $1', cfg.target_detailed) USING p_index;

                EXECUTE format('
                    INSERT INTO %I (
                        partition_index, partition_start_time_ms, asset_id, total_count,
                        count_miss_0, count_miss_1, count_miss_2, count_miss_3, count_miss_4, 
                        count_miss_5, count_miss_6, count_miss_7, count_miss_8, count_miss_9, 
                        count_miss_10, count_miss_11, count_miss_12, count_miss_13
                    )
                    SELECT 
                        FLOOR((found_time_ms - %s) / %s) AS partition_index,
                        %s + (FLOOR((found_time_ms - %s) / %s) * %s) AS partition_start_time_ms,
                        asset_id,
                        COUNT(*) AS total_count,
                        COUNT(*) FILTER (WHERE false_misses = 0) AS count_miss_0,
                        COUNT(*) FILTER (WHERE false_misses = 1) AS count_miss_1,
                        COUNT(*) FILTER (WHERE false_misses = 2) AS count_miss_2,
                        COUNT(*) FILTER (WHERE false_misses = 3) AS count_miss_3,
                        COUNT(*) FILTER (WHERE false_misses = 4) AS count_miss_4,
                        COUNT(*) FILTER (WHERE false_misses = 5) AS count_miss_5,
                        COUNT(*) FILTER (WHERE false_misses = 6) AS count_miss_6,
                        COUNT(*) FILTER (WHERE false_misses = 7) AS count_miss_7,
                        COUNT(*) FILTER (WHERE false_misses = 8) AS count_miss_8,
                        COUNT(*) FILTER (WHERE false_misses = 9) AS count_miss_9,
                        COUNT(*) FILTER (WHERE false_misses = 10) AS count_miss_10,
                        COUNT(*) FILTER (WHERE false_misses = 11) AS count_miss_11,
                        COUNT(*) FILTER (WHERE false_misses = 12) AS count_miss_12,
                        COUNT(*) FILTER (WHERE false_misses = 13) AS count_miss_13
                    FROM %I 
                    GROUP BY 1, 2, 3',
                    cfg.target_detailed,
                    ANCHOR_TIME, PARTITION_SIZE, 
                    ANCHOR_TIME, ANCHOR_TIME, PARTITION_SIZE, PARTITION_SIZE,
                    p_name
                );
                GET DIAGNOSTICS rows_detailed = ROW_COUNT;
            END IF;

            RAISE NOTICE 'Finished: % raw misses, % simple counts, % detailed aggs in %', 
                         rows_raw, rows_simple, rows_detailed, (clock_timestamp() - start_ts);

        END LOOP; -- End Partition Loop
    END LOOP; -- End Config Loop
    
    RAISE NOTICE 'All tables processed successfully.';
END $$;