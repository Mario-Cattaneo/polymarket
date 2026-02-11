#!/usr/bin/env python3
"""
Clustering and power-law regression analysis: slippage vs order size
Performs GMM clustering on log(order_size) vs log(slippage) and fits power-law 
regressions (linear in log-log space) for each cluster.
"""

import os
import asyncio
import asyncpg
import logging
import json
import matplotlib.pyplot as plt
import numpy as np
from decimal import Decimal, getcontext
from eth_abi import decode
from scipy import stats
import seaborn as sns
from sklearn.mixture import GaussianMixture
from sklearn.metrics import silhouette_score

# ----------------------------------------------------------------
# CONFIGURATION
# ----------------------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
logger = logging.getLogger()
getcontext().prec = 36

# --- Database Credentials ---
PG_HOST = os.getenv("PG_SOCKET")
PG_PORT = os.getenv("POLY_PG_PORT")
DB_NAME = os.getenv("POLY_DB")
DB_USER = os.getenv("POLY_DB_CLI")
DB_PASS = os.getenv("POLY_DB_CLI_PASS")

# --- Timestamp Interpolation Constants ---
REF_BLOCK_NUMBER = 81372608
REF_UNIX_TIMESTAMP = 1767864474
SECONDS_PER_BLOCK = 2

# --- Block Range Configuration ---
START_BLOCK_NR = 79172085
END_BLOCK_NR = 81225971
BLOCK_BATCH_SIZE = 50_000

# --- Regression Filter Range ---
REGRESSION_START_TIMESTAMP = 0 #((START_BLOCK_NR - REF_BLOCK_NUMBER) * SECONDS_PER_BLOCK) + REF_UNIX_TIMESTAMP + 24 * 60 * 60
REGRESSION_END_TIMESTAMP = None
# --- Slippage Epsilon ---
SLIPPAGE_EPSILON = 0.0001  # Minimum slippage threshold; below this is treated as zero
# --- Constants ---
USDC_SCALAR = Decimal("1_000_000")
SHARE_SCALAR = Decimal("1_000_000")

# ----------------------------------------------------------------
# HELPER & DATA PROCESSING FUNCTIONS
# ----------------------------------------------------------------

def block_to_timestamp(block_number: int) -> int:
    return ((block_number - REF_BLOCK_NUMBER) * SECONDS_PER_BLOCK) + REF_UNIX_TIMESTAMP

async def get_block_bounds(conn) -> tuple:
    """Get min and max block numbers from settlements table."""
    row = await conn.fetchrow("SELECT MIN(block_number) as min_b, MAX(block_number) as max_b FROM settlements")
    return row['min_b'], row['max_b']

async def fetch_batch(conn, start_block: int, end_block: int) -> list:
    """Fetch settlements in a block range."""
    return await conn.fetch(
        "SELECT block_number, transaction_hash, type, trade FROM settlements WHERE block_number >= $1 AND block_number < $2 ORDER BY block_number ASC",
        start_block, end_block
    )

def decode_order_filled(data: str, topics: list) -> dict:
    """Decode OrderFilled event."""
    try:
        data_hex = bytes.fromhex(data[2:] if data.startswith('0x') else data)
        decoded = decode(['uint256', 'uint256', 'uint256', 'uint256', 'uint256'], data_hex)
        
        maker = '0x' + topics[2][-40:] if len(topics) > 2 else None
        taker = '0x' + topics[3][-40:] if len(topics) > 3 else None
        maker_asset_id = int(decoded[0])
        taker_asset_id = int(decoded[1])
        making = int(decoded[2])
        taking = int(decoded[3])
        fee = int(decoded[4])
        
        return {
            'maker': maker,
            'taker': taker,
            'makerAssetId': maker_asset_id,
            'takerAssetId': taker_asset_id,
            'making': making,
            'taking': taking,
            'fee': fee
        }
    except Exception as e:
        logger.debug(f"Failed to decode OrderFilled: {e}")
        return {}

async def process_batch_for_regression(settlements: list) -> tuple:
    """
    Extract discrete (inflow, order_size, slippage) tuples from settlements.
    Returns (timestamps, inflows, order_sizes, slippages, settlement_data).
    """
    EXCHANGE_ADDRESS = "0xC5d563A36AE78145C45a50134d48A1215220f80a"
    
    timestamps = []
    inflows = []
    order_sizes = []
    slippages = []
    settlement_data = []
    count = 0
    
    for row in settlements:
        try:
            trade = json.loads(row['trade'])
            
            if not trade.get('orders_filled') or len(trade.get('orders_filled', [])) < 2:
                continue
            
            # Decode all OrderFilled events
            all_fills = []
            exchange_fill = None
            for idx, fill in enumerate(trade.get('orders_filled', [])):
                decoded_fill = decode_order_filled(fill['data'], fill['topics'])
                if decoded_fill:
                    is_exchange_fill = (decoded_fill['taker'].lower() == EXCHANGE_ADDRESS.lower())
                    decoded_fill['is_exchange'] = is_exchange_fill
                    all_fills.append(decoded_fill)
                    
                    if is_exchange_fill:
                        exchange_fill = decoded_fill
            
            if not exchange_fill:
                continue
            
            # Determine taker direction (from exchange fill)
            if exchange_fill['makerAssetId'] == 0:
                taker_is_buying = True
            else:
                taker_is_buying = False
            
            # Get taker USDC amount and tokens from exchange fill
            exchange_making = Decimal(exchange_fill['making'])
            exchange_taking = Decimal(exchange_fill['taking'])
            
            # taker_usdc_amount = min(making, taking)
            # taker_tokens = max(making, taking)
            taker_usdc_amount = min(exchange_making, exchange_taking) / USDC_SCALAR
            taker_tokens = max(exchange_making, exchange_taking) / SHARE_SCALAR
            
            # Process maker fills
            maker_fills = [f for f in all_fills if not f['is_exchange']]
            
            if len(maker_fills) < 1:
                continue
            
            # Calculate prices for all maker fills
            maker_prices = []
            for fill in maker_fills:
                making_raw = Decimal(fill['making'])
                taking_raw = Decimal(fill['taking'])
                
                # Determine maker side from makerAssetId
                maker_is_buying = (fill['makerAssetId'] == 0)
                
                # price_per_share = min/max
                price_per_share = min(making_raw, taking_raw) / max(making_raw, taking_raw)
                
                # If maker is on same side as taker, flip price
                if maker_is_buying == taker_is_buying:
                    price_per_share = Decimal(1) - price_per_share
                
                maker_prices.append(price_per_share)
            
            # Only compute slippage if we have at least 1 maker fill
            if len(maker_prices) >= 1:
                # best_price = minimum price (buying at min price is best)
                best_price_per_share = min(maker_prices)
                
                # slippage = taker_usdc_amount - best_price_per_share * taker_tokens
                expected_cost = best_price_per_share * taker_tokens
                tx_slippage = taker_usdc_amount - expected_cost
                
                timestamp = block_to_timestamp(row['block_number'])
                
                # Order size (in shares)
                order_size_shares = float(taker_tokens)
                
                # Store discrete measurement
                timestamps.append(timestamp)
                inflows.append(float(taker_usdc_amount))
                order_sizes.append(order_size_shares)
                slippages.append(float(tx_slippage))
                settlement_data.append({
                    'transaction_hash': row['transaction_hash'],
                    'block_number': row['block_number'],
                    'trade': trade
                })
                count += 1

        except Exception as e:
            continue
    
    return timestamps, inflows, order_sizes, slippages, settlement_data, count

def filter_data_by_timestamp(timestamps: list, inflows: list, order_sizes: list, slippages: list, settlement_data: list) -> tuple:
    """Filter data based on timestamp range."""
    filtered_ts = []
    filtered_inf = []
    filtered_size = []
    filtered_slip = []
    filtered_settlement_data = []
    
    for ts, inf, size, slip, sett_data in zip(timestamps, inflows, order_sizes, slippages, settlement_data):
        if REGRESSION_START_TIMESTAMP is not None and ts < REGRESSION_START_TIMESTAMP:
            continue
        if REGRESSION_END_TIMESTAMP is not None and ts > REGRESSION_END_TIMESTAMP:
            continue
        filtered_ts.append(ts)
        filtered_inf.append(inf)
        filtered_size.append(size)
        filtered_slip.append(slip)
        filtered_settlement_data.append(sett_data)
    
    return filtered_ts, filtered_inf, filtered_size, filtered_slip, filtered_settlement_data

def test_power_law_fit(X: np.ndarray, y: np.ndarray, name: str = ""):
    """Fit a power-law: slippage = C * usdc_inflow^alpha by regressing logs.
    Returns dict with exponent `alpha`, prefactor `C`, R², RMSE, p-value and sample counts.
    """
    logger.info(f"\nTESTING POWER-LAW FIT ({name}):")

    # Keep only positive X and positive y
    mask = (X > 0) & (y > 0)
    Xp = X[mask]
    yp = y[mask]

    if len(Xp) < 2:
        logger.info("  Not enough positive samples for power-law fit")
        return None

    logX = np.log(Xp)
    logY = np.log(yp)

    slope, intercept, r_value, p_value, std_err = stats.linregress(logX, logY)

    # Predictions and metrics on original and log scales
    y_pred_log = slope * logX + intercept
    y_pred = np.exp(y_pred_log)
    rmse = np.sqrt(np.mean((yp - y_pred) ** 2))
    rmse_log = np.sqrt(np.mean((logY - y_pred_log) ** 2))

    alpha = float(slope)
    prefactor = float(np.exp(intercept))

    # Calculate intercept std_err for C uncertainty
    n_used = len(Xp)
    residuals = logY - y_pred_log
    sigma_residual = np.sqrt(np.sum(residuals**2) / (n_used - 2))  # unbiased residual std error
    mean_logX = np.mean(logX)
    ss_logX = np.sum((logX - mean_logX)**2)
    std_err_intercept = sigma_residual * np.sqrt(1.0 / n_used + mean_logX**2 / ss_logX)
    
    # Confidence intervals for intercept and C
    ci_intercept_lower = intercept - 1.96 * std_err_intercept
    ci_intercept_upper = intercept + 1.96 * std_err_intercept
    prefactor_ci_lower = float(np.exp(ci_intercept_lower))
    prefactor_ci_upper = float(np.exp(ci_intercept_upper))

    n_total = len(X)
    n_excluded = n_total - n_used

    logger.info(f"  Samples used (positive X & y): {n_used:,}  Excluded: {n_excluded:,}")
    logger.info(f"  Power-law exponent (alpha): {alpha:.6f} (std_err={std_err:.6f}, p={p_value:.2e})")
    logger.info(f"  Alpha 95% CI: [{alpha - 1.96*std_err:.6f}, {alpha + 1.96*std_err:.6f}]")
    logger.info(f"  Prefactor (C): {prefactor:.6e} (std_err_intercept={std_err_intercept:.6f})")
    logger.info(f"  C 95% CI: [{prefactor_ci_lower:.6e}, {prefactor_ci_upper:.6e}]")
    logger.info(f"  R² (log-log): {r_value**2:.6f}")
    logger.info(f"  RMSE (original scale): {rmse:.6e}")
    logger.info(f"  RMSE (log scale): {rmse_log:.6e}")

    return {
        'alpha': alpha,
        'prefactor': prefactor,
        'r2': r_value**2,
        'rmse': rmse,
        'rmse_log': rmse_log,
        'p_value': p_value,
        'std_err': std_err,
        'std_err_intercept': std_err_intercept,
        'alpha_ci': (alpha - 1.96*std_err, alpha + 1.96*std_err),
        'prefactor_ci': (prefactor_ci_lower, prefactor_ci_upper),
        'n_samples': n_used,
        'n_excluded': n_excluded,
        'X': Xp,
        'y': yp,
        'y_pred': y_pred,
        'logX': logX,
        'logY': logY,
        'y_pred_log': y_pred_log
    }

def analyze_and_cluster(inflows: np.ndarray, slippages: np.ndarray, settlement_data: list, n_clusters: int = 3):
    """Cluster (log USDC_inflow, log slippage) into `n_clusters` groups and produce diagnostic plots.

    Returns a dict with cluster assignments, GMM model, and summary statistics.
    """
    logger.info(f"\nCLUSTERING: fitting {n_clusters} components on positive USDC inflow & slippage samples")
    logger.info("Note: `inflow` = min(making,taking)/USDC_SCALAR (USDC from the taker-facing exchange fill)")

    xs = np.array(inflows)
    ys = np.array(slippages)

    # Mask positive samples (exclude zeros for log clustering)
    mask_pos = (xs > 0) & (ys > 0)
    n_total = len(xs)
    n_pos = int(np.sum(mask_pos))
    n_zero_or_neg = n_total - n_pos
    logger.info(f"  Total samples: {n_total:,}, positive (used): {n_pos:,}, excluded (zero/neg): {n_zero_or_neg:,}")

    if n_pos < n_clusters:
        logger.warning("Not enough positive samples to form requested clusters")
        return None

    logX = np.log(xs[mask_pos])
    logY = np.log(ys[mask_pos])
    feats = np.column_stack([logX, logY])

    # Fit Gaussian Mixture Model
    gmm = GaussianMixture(n_components=n_clusters, covariance_type='full', random_state=0)
    gmm.fit(feats)
    labels = gmm.predict(feats)
    probs = gmm.predict_proba(feats)

    bic = gmm.bic(feats)
    aic = gmm.aic(feats)

    sil_score = None
    try:
        if n_pos >= 10 and n_clusters > 1:
            sil_score = silhouette_score(feats, labels)
    except Exception:
        sil_score = None

    # Prepare summary per cluster
    cluster_summary = []
    for k in range(n_clusters):
        idx = np.where(labels == k)[0]
        count = len(idx)
        median_size = float(np.median(np.exp(logX[idx]))) if count > 0 else 0.0
        median_slip = float(np.median(np.exp(logY[idx]))) if count > 0 else 0.0

        # Collect top asset ids by frequency for maker fills in this cluster
        maker_assets = {}
        taker_assets = {}
        cluster_settlements = []
        # Map masked indices back to settlement_data
        masked_indices = np.nonzero(mask_pos)[0]
        for local_i in idx:
            orig_i = masked_indices[local_i]
            cluster_settlements.append((orig_i, settlement_data[orig_i]))
            trade = settlement_data[orig_i]['trade']
            for fill in trade.get('orders_filled', []):
                decoded = decode_order_filled(fill['data'], fill['topics'])
                if not decoded:
                    continue
                # Count maker/taker asset ids
                maker_assets[decoded.get('makerAssetId')] = maker_assets.get(decoded.get('makerAssetId'), 0) + 1
                taker_assets[decoded.get('takerAssetId')] = taker_assets.get(decoded.get('takerAssetId'), 0) + 1

        # Sort top assets
        top_maker = sorted(maker_assets.items(), key=lambda x: x[1], reverse=True)[:3]
        top_taker = sorted(taker_assets.items(), key=lambda x: x[1], reverse=True)[:3]

        cluster_summary.append({
            'cluster': k,
            'count': count,
            'median_usdc_inflow': median_size,
            'median_slippage': median_slip,
            'top_maker_assets': top_maker,
            'top_taker_assets': top_taker,
            'example_settlements': cluster_settlements[:3]
        })

    # --- Per-cluster power-law fits (for clusters with enough data) ---
    cluster_fits = {}
    for s in cluster_summary:
        k = s['cluster']
        if s['count'] < 10:
            logger.info(f"Skipping power-law fit for cluster {k} (too few samples: {s['count']})")
            continue
        sel = (labels == k)
        xs_k = xs[mask_pos][sel]
        ys_k = ys[mask_pos][sel]
        fit_k = test_power_law_fit(xs_k, ys_k, f"Cluster {k}")
        cluster_fits[k] = fit_k

    # --- Plot: cluster density visualization with regression lines (log-log) ---
    plt.rcParams.update({'font.size': 20})
    fig, ax = plt.subplots(figsize=(14, 10))
    colors = sns.color_palette('deep', n_clusters)

    # Plot all points faint as background (including zeros) but don't add legend entry
    ax.scatter(xs, ys, s=8, alpha=0.03, color='lightgray', zorder=1)

    # Plot clustered positive points colored
    masked_orig_idx = np.nonzero(mask_pos)[0]
    for k in range(n_clusters):
        sel = (labels == k)
        ax.scatter(xs[masked_orig_idx[sel]], ys[masked_orig_idx[sel]], s=12, alpha=0.45, color=colors[k], label=f'Cluster {k} (n={cluster_summary[k]["count"]:,})', edgecolors='none', zorder=50)

    # Add regression lines for clusters that have fits
    for k in range(n_clusters):
        if k in cluster_fits and cluster_fits[k] is not None:
            sel = (labels == k)
            Ck = cluster_fits[k]['prefactor']
            alphak = cluster_fits[k]['alpha']
            
            # Get x range from this cluster's data
            xmin = xs[masked_orig_idx[sel]].min()
            xmax = xs[masked_orig_idx[sel]].max()
            
            if xmin > 0 and xmax > 0:
                x_line = np.logspace(np.log10(xmin), np.log10(xmax), 200)
                y_line = Ck * (x_line ** alphak)
                ax.plot(x_line, y_line, color='black', linewidth=3.0, linestyle='-', zorder=200)

    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlim(1e-1, 1e7)
    ax.set_xlabel('USDC Inflow', fontsize=26, fontweight='bold')
    ax.set_ylabel('Slippage (USDC)', fontsize=26, fontweight='bold')
    ax.tick_params(axis='both', which='major', labelsize=22)
    ax.set_title(f'Clustered Slippage / USDC Inflow (n={n_clusters})', fontsize=28, fontweight='bold')

    # Legend with larger colored texts (no marker/handle on the left)
    from matplotlib.lines import Line2D
    labels = [f'Cluster {k} (n={cluster_summary[k]["count"]:,})' for k in range(n_clusters)]
    proxy_handles = [Line2D([], [], linestyle='', marker='', color='none') for _ in range(n_clusters)]
    leg = ax.legend(proxy_handles, labels, loc='lower right', bbox_to_anchor=(0.98, 0.02), prop={'size': 24}, handlelength=0, handletextpad=0.6)
    leg.get_frame().set_alpha(0.92)
    for i, text in enumerate(leg.get_texts()):
        text.set_color(colors[i])
        text.set_fontsize(24)

    plt.tight_layout()
    cluster_plot_file = 'clusters_density_loglog.png'
    plt.savefig(cluster_plot_file, dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"✅ Cluster density plot with power-law regression lines saved to {cluster_plot_file}")

    # Save CSV summary
    import csv
    csv_file = 'cluster_summary.csv'
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['cluster', 'count', 'median_usdc_inflow', 'median_slippage', 'top_maker_assets', 'top_taker_assets'])
        for s in cluster_summary:
            writer.writerow([s['cluster'], s['count'], s['median_usdc_inflow'], s['median_slippage'], str(s['top_maker_assets']), str(s['top_taker_assets'])])
    logger.info(f"✅ Cluster CSV summary saved to {csv_file}")

    return {
        'gmm': gmm,
        'labels': labels,
        'probs': probs,
        'bic': bic,
        'aic': aic,
        'silhouette': sil_score,
        'summary': cluster_summary,
        'fits': cluster_fits,
        'masked_indices': masked_indices
    }

def compute_statistics(inflows: np.ndarray, slippages: np.ndarray) -> dict:
    """
    Compute comprehensive statistics for:
    1. Taker USDC Amount (inflows)
    2. Slippage
    3. Slippage / Taker USDC Amount ratio (measured individually over all)
    Returns dictionary with variance, std dev, and expected value for each.
    """
    # Compute ratio (slippage / inflow) for each settlement individually
    # Handle division by zero by filtering out inflows that are 0
    ratio = np.divide(slippages, inflows, where=inflows != 0, out=np.full_like(slippages, np.nan))
    # Remove NaN values from ratio for statistical calculation
    ratio_clean = ratio[~np.isnan(ratio)]
    
    stats_dict = {
        'taker_usdc_amount': {
            'expected_value': np.mean(inflows),
            'variance': np.var(inflows, ddof=1),
            'std_dev': np.std(inflows, ddof=1),
            'count': len(inflows)
        },
        'slippage': {
            'expected_value': np.mean(slippages),
            'variance': np.var(slippages, ddof=1),
            'std_dev': np.std(slippages, ddof=1),
            'count': len(slippages)
        },
        'slippage_per_usdc': {
            'expected_value': np.mean(ratio_clean),
            'variance': np.var(ratio_clean, ddof=1),
            'std_dev': np.std(ratio_clean, ddof=1),
            'count': len(ratio_clean)
        }
    }
    
    return stats_dict

def log_statistics_summary(stats_dict: dict):
    """Log the computed statistics in a formatted way."""
    logger.info("\n" + "="*70)
    logger.info("STATISTICAL ANALYSIS (Filtered Time Range)")
    logger.info("="*70)
    
    # Taker USDC Amount
    logger.info("\n1. TAKER USDC AMOUNT:")
    logger.info(f"   Expected Value (Mean):  ${stats_dict['taker_usdc_amount']['expected_value']:.6f}")
    logger.info(f"   Variance:               {stats_dict['taker_usdc_amount']['variance']:.10f}")
    logger.info(f"   Std Deviation:          ${stats_dict['taker_usdc_amount']['std_dev']:.6f}")
    logger.info(f"   Sample Size:            {stats_dict['taker_usdc_amount']['count']:,}")
    
    # Slippage
    logger.info("\n2. SLIPPAGE:")
    logger.info(f"   Expected Value (Mean):  ${stats_dict['slippage']['expected_value']:.6f}")
    logger.info(f"   Variance:               {stats_dict['slippage']['variance']:.10f}")
    logger.info(f"   Std Deviation:          ${stats_dict['slippage']['std_dev']:.6f}")
    logger.info(f"   Sample Size:            {stats_dict['slippage']['count']:,}")
    
    # Slippage / USDC Amount Ratio
    logger.info("\n3. SLIPPAGE / TAKER USDC AMOUNT:")
    logger.info(f"   Expected Value (Mean):  {stats_dict['slippage_per_usdc']['expected_value']:.6f}")
    logger.info(f"   Variance:               {stats_dict['slippage_per_usdc']['variance']:.10f}")
    logger.info(f"   Std Deviation:          {stats_dict['slippage_per_usdc']['std_dev']:.6f}")
    logger.info(f"   Sample Size:            {stats_dict['slippage_per_usdc']['count']:,}")
    
    logger.info("\n" + "="*70 + "\n")

# ----------------------------------------------------------------
# MAIN EXECUTION
# ----------------------------------------------------------------

async def main():
    conn = None
    try:
        conn = await asyncpg.connect(host=PG_HOST, port=int(PG_PORT), database=DB_NAME, user=DB_USER, password=DB_PASS)
        
        min_blk, max_blk = await get_block_bounds(conn)
        if not min_blk:
            logger.error("No settlement records found.")
            return
        
        start_blk = START_BLOCK_NR if START_BLOCK_NR is not None else min_blk
        end_blk = END_BLOCK_NR if END_BLOCK_NR is not None else max_blk
        
        logger.info(f"Processing settlements from block {start_blk} to {end_blk}")
        
        # Batch processing
        all_timestamps = []
        all_inflows = []
        all_order_sizes = []
        all_slippages = []
        all_settlement_data = []
        total_processed = 0
        
        curr_blk = start_blk
        
        while curr_blk < end_blk:
            next_blk = min(curr_blk + BLOCK_BATCH_SIZE, end_blk)
            
            settlements = await fetch_batch(conn, curr_blk, next_blk)
            if settlements:
                timestamps, inflows, order_sizes, slippages, settlement_data, count = await process_batch_for_regression(settlements)
                
                all_timestamps.extend(timestamps)
                all_inflows.extend(inflows)
                all_order_sizes.extend(order_sizes)
                all_slippages.extend(slippages)
                all_settlement_data.extend(settlement_data)
                total_processed += count
                
                logger.info(f"  Blocks {curr_blk:,}-{next_blk:,}: {count} valid settlements")
            
            curr_blk = next_blk
        
        logger.info(f"\nTotal settlements: {total_processed}")
        
        # Filter by timestamp
        if REGRESSION_START_TIMESTAMP or REGRESSION_END_TIMESTAMP:
            filtered_ts, filtered_inflows, filtered_sizes, filtered_slippages, filtered_settlement_data = filter_data_by_timestamp(
                all_timestamps, all_inflows, all_order_sizes, all_slippages, all_settlement_data
            )
            logger.info(f"After timestamp filtering: {len(filtered_inflows)} samples")
        else:
            filtered_inflows = all_inflows
            filtered_sizes = all_order_sizes
            filtered_slippages = all_slippages
            filtered_settlement_data = all_settlement_data
            filtered_settlement_data = all_settlement_data
        
        if len(filtered_inflows) < 2:
            logger.error("Not enough samples for regression")
            return
        
        # Convert to numpy
        inflows_arr = np.array(filtered_inflows)
        order_sizes_arr = np.array(filtered_sizes)
        slippages_arr = np.array(filtered_slippages)
        
        # --- ZERO-SLIPPAGE DIAGNOSTICS (including epsilon threshold) ---
        zero_mask = slippages_arr < SLIPPAGE_EPSILON  # Includes slippage == 0 and slippage < epsilon
        n_zero = int(np.sum(zero_mask))
        pct_zero = (n_zero / len(slippages_arr)) * 100 if len(slippages_arr) > 0 else 0.0
        max_taker_zero = float(inflows_arr[zero_mask].max()) if n_zero > 0 else 0.0
        logger.info("\nZERO-SLIPPAGE DIAGNOSTICS (including epsilon threshold):")
        logger.info(f"  SLIPPAGE_EPSILON: {SLIPPAGE_EPSILON}")
        logger.info(f"  Count with slippage < epsilon: {n_zero:,} ({pct_zero:.6f}%)")
        logger.info(f"  Max taker USDC amount among zero-slip settlements: ${max_taker_zero:.6f}")

        # --- MAX SLIPPAGE / TAKER_USDC DIAGNOSTIC ---
        # Compute slippage per dollar where inflow != 0
        valid_mask = inflows_arr != 0
        if np.any(valid_mask):
            ratios = np.divide(slippages_arr, inflows_arr, out=np.full_like(slippages_arr, np.nan), where=valid_mask)
            if np.all(np.isnan(ratios)):
                logger.info("No valid slippage/inflow ratios available")
            else:
                idx_max = int(np.nanargmax(ratios))
                max_ratio = float(ratios[idx_max])
                taker_amount_at_max = float(inflows_arr[idx_max])
                slippage_at_max = float(slippages_arr[idx_max])
                logger.info("\nMAX SLIPPAGE-per-TAKER USD DIAGNOSTIC:")
                logger.info(f"  Max slippage/taker_usdc ratio: {max_ratio:.6f}")
                logger.info(f"  Corresponding taker USDC amount: ${taker_amount_at_max:.6f}")
                logger.info(f"  Corresponding slippage: ${slippage_at_max:.6f}")
                
                # NEW: Count settlements with ratio >= 1 (impossible case: slippage >= taker amount)
                invalid_ratio_mask = ratios >= 1.0
                count_invalid_ratio = int(np.nansum(invalid_ratio_mask))
                pct_invalid_ratio = (count_invalid_ratio / len(slippages_arr)) * 100 if len(slippages_arr) > 0 else 0.0
                logger.info(f"\n⚠️  RATIO >= 1.0 DIAGNOSTICS (IMPOSSIBLE: slippage >= taker amount):")
                logger.info(f"  Count with ratio >= 1.0: {count_invalid_ratio:,} ({pct_invalid_ratio:.6f}%)")
                
                if count_invalid_ratio > 0:
                    # Find top 3 largest ratios
                    top_n = min(3, count_invalid_ratio)
                    invalid_indices = np.where(invalid_ratio_mask)[0]
                    top_indices = invalid_indices[np.argsort(ratios[invalid_indices])[-top_n:][::-1]]
                    
                    logger.info(f"\n  Top {top_n} samples with highest ratio >= 1.0:")
                    for rank, idx in enumerate(top_indices, 1):
                        logger.info(f"\n  [{rank}] Ratio: {ratios[idx]:.6f}, Taker USDC: ${inflows_arr[idx]:.6f}, Slippage: ${slippages_arr[idx]:.6f}")
                        
                        # Print decoded settlement data
                        if idx < len(filtered_settlement_data):
                            sett = filtered_settlement_data[idx]
                            logger.info(f"       TX Hash: {sett['transaction_hash']}")
                            logger.info(f"       Block: {sett['block_number']}")
                            
                            # Decode and print OrderFilled events
                            trade = sett['trade']
                            if trade.get('orders_filled'):
                                logger.info(f"       Decoded OrderFilled Events:")
                                for fill_idx, fill in enumerate(trade.get('orders_filled', []), 1):
                                    decoded_fill = decode_order_filled(fill['data'], fill['topics'])
                                    if decoded_fill:
                                        logger.info(f"         [{fill_idx}] Maker: {decoded_fill.get('maker')}")
                                        logger.info(f"             Taker: {decoded_fill.get('taker')}")
                                        logger.info(f"             Making: {decoded_fill.get('making')}, Taking: {decoded_fill.get('taking')}")
                                        logger.info(f"             MakerAssetId: {decoded_fill.get('makerAssetId')}, TakerAssetId: {decoded_fill.get('takerAssetId')}")
                                        logger.info(f"             Fee: {decoded_fill.get('fee')}")
                        
        else:
            logger.info("No non-zero taker inflows to compute slippage-per-dollar ratio")
        
        # --- FILTER BY EPSILON THRESHOLD ---
        nonzero_mask = slippages_arr >= SLIPPAGE_EPSILON
        n_nonzero = int(np.sum(nonzero_mask))
        logger.info(f"\nFiltered for analysis (slippage >= {SLIPPAGE_EPSILON}): {n_nonzero:,} samples")
        
        inflows_filtered = inflows_arr[nonzero_mask]
        slippages_filtered = slippages_arr[nonzero_mask]
        order_sizes_filtered = order_sizes_arr[nonzero_mask]
        settlement_data_filtered = [d for i, d in enumerate(filtered_settlement_data) if nonzero_mask[i]]
        
        # --- POWER-LAW FIT ON FILTERED DATA ---
        if n_nonzero >= 2:
            fit_result = test_power_law_fit(inflows_filtered, slippages_filtered, "Overall (epsilon-filtered)")
            
            # --- PLOT: Simple power-law fit (epsilon-filtered data, log-log) ---
            plt.rcParams.update({'font.size': 20})
            fig, ax = plt.subplots(figsize=(14, 10))
            
            # Plot all filtered points
            ax.scatter(inflows_filtered, slippages_filtered, s=12, alpha=0.45, color='steelblue', edgecolors='none', zorder=50)
            
            # Add regression line if fit was successful
            if fit_result is not None:
                C = fit_result['prefactor']
                alpha = fit_result['alpha']
                xmin = inflows_filtered.min()
                xmax = inflows_filtered.max()
                
                if xmin > 0 and xmax > 0:
                    x_line = np.logspace(np.log10(xmin), np.log10(xmax), 200)
                    y_line = C * (x_line ** alpha)
                    ax.plot(x_line, y_line, color='red', linewidth=3.0, linestyle='-', zorder=200, label=f'Power-law fit (α={alpha:.3f})')
            
            ax.set_xscale('log')
            ax.set_yscale('log')
            ax.set_xlim(1e-3, 1e7)
            ax.set_xlabel('USDC Inflow', fontsize=26, fontweight='bold')
            ax.set_ylabel('Slippage (USDC)', fontsize=26, fontweight='bold')
            ax.tick_params(axis='both', which='major', labelsize=22)
            ax.set_title(f'Slippage vs USDC Inflow (n={n_nonzero:,})', fontsize=28, fontweight='bold')
            ax.legend(fontsize=22, loc='upper left')
            
            plt.tight_layout()
            plot_file = 'slippage_power_law.png'
            plt.savefig(plot_file, dpi=300, bbox_inches='tight')
            plt.close()
            logger.info(f"✅ Power-law plot (epsilon-filtered) saved to {plot_file}")
        else:
            logger.warning(f"Not enough samples above epsilon threshold for power-law fit")
        
        # Compute and log comprehensive statistics
        stats_dict = compute_statistics(inflows_arr, slippages_arr)
        log_statistics_summary(stats_dict)
        
        # Summary statistics
        logger.info("\n### SUMMARY STATISTICS ###\n")
        logger.info(f"Inflow:      mean=${inflows_arr.mean():.2f}, median=${np.median(inflows_arr):.2f}, std=${inflows_arr.std():.2f}")
        logger.info(f"Order Size:  mean={order_sizes_arr.mean():.2f} shares, median={np.median(order_sizes_arr):.2f}, std={order_sizes_arr.std():.2f}")
        logger.info(f"Slippage:    mean=${slippages_arr.mean():.4f}, median=${np.median(slippages_arr):.4f}, std=${slippages_arr.std():.4f}")
        logger.info("")
        
        # Filtered summary (epsilon-filtered)
        logger.info("\n### FILTERED STATISTICS (slippage >= epsilon) ###\n")
        if n_nonzero > 0:
            logger.info(f"Inflow:      mean=${inflows_filtered.mean():.2f}, median=${np.median(inflows_filtered):.2f}, std=${inflows_filtered.std():.2f}")
            logger.info(f"Order Size:  mean={order_sizes_filtered.mean():.2f} shares, median={np.median(order_sizes_filtered):.2f}, std={order_sizes_filtered.std():.2f}")
            logger.info(f"Slippage:    mean=${slippages_filtered.mean():.4f}, median=${np.median(slippages_filtered):.4f}, std=${slippages_filtered.std():.4f}")
        else:
            logger.info("No samples above epsilon threshold.")
        logger.info("")
        
    finally:
        if conn:
            await conn.close()

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("Database credentials not set.")
    else:
        asyncio.run(main())
