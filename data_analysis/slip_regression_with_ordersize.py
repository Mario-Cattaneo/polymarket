#!/usr/bin/env python3
"""
Extended regression analysis: slippage with order size
Performs multiple regression: slippage = β₀ + β₁*inflow + β₂*order_size + ε
Also includes stratified analysis by order size buckets.
"""

import os
import asyncio
import asyncpg
import logging
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import pandas as pd
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
END_BLOCK_NR = 79172085 + 100_000 #81225971
BLOCK_BATCH_SIZE = 50_000

# --- Regression Filter Range ---
REGRESSION_START_TIMESTAMP = 0 #((START_BLOCK_NR - REF_BLOCK_NUMBER) * SECONDS_PER_BLOCK) + REF_UNIX_TIMESTAMP + 24 * 60 * 60
REGRESSION_END_TIMESTAMP = None

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

def perform_regression(inflows: list, order_sizes: list, slippages: list) -> dict:
    """
    Perform multiple regression: slippage = β₀ + β₁*inflow + β₂*order_size + ε
    """
    from sklearn.linear_model import LinearRegression
    from sklearn.metrics import r2_score
    
    X = np.array([inflows, order_sizes]).T  # shape: (n_samples, 2)
    y = np.array(slippages)
    
    model = LinearRegression()
    model.fit(X, y)
    
    y_pred = model.predict(X)
    residuals = y - y_pred
    
    r2 = r2_score(y, y_pred)
    rmse = np.sqrt(np.mean(residuals ** 2))
    
    # Calculate p-values using scipy
    n = len(y)
    k = 2  # number of predictors
    mse = np.sum(residuals ** 2) / (n - k - 1)
    
    # Standard errors
    X_with_const = np.column_stack([np.ones(n), X])
    var_covar = mse * np.linalg.inv(X_with_const.T @ X_with_const)
    se = np.sqrt(np.diag(var_covar))
    
    # T-stats and p-values
    t_stats = np.concatenate([[model.intercept_], model.coef_]) / se
    p_values = 2 * (1 - stats.t.cdf(np.abs(t_stats), n - k - 1))
    
    return {
        'intercept': model.intercept_,
        'coef_inflow': model.coef_[0],
        'coef_order_size': model.coef_[1],
        'r2': r2,
        'rmse': rmse,
        'p_values': p_values,
        'std_errors': se,
        'n_samples': n,
        'X': X,
        'y': y,
        'y_pred': y_pred
    }

def perform_simple_regression(X: np.ndarray, y: np.ndarray) -> dict:
    """Simple linear regression for single predictor."""
    slope, intercept, r_value, p_value, std_err = stats.linregress(X, y)
    y_pred = slope * X + intercept
    residuals = y - y_pred
    rmse = np.sqrt(np.mean(residuals ** 2))
    
    return {
        'slope': slope,
        'intercept': intercept,
        'r_value': r_value,
        'r_squared': r_value ** 2,
        'p_value': p_value,
        'std_err': std_err,
        'rmse': rmse,
        'n_samples': len(X),
        'y_pred': y_pred
    }

def log_regression_summary(results: dict, title: str = "REGRESSION RESULTS"):
    """Log regression statistics."""
    logger.info("\n" + "="*70)
    logger.info(f"{title}")
    logger.info("="*70)
    
    if 'slope' in results:  # Simple regression
        logger.info(f"Intercept:              {results['intercept']:.8f}")
        logger.info(f"Slope:                  {results['slope']:.8f}")
        logger.info(f"R²:                     {results['r_squared']:.6f}")
        logger.info(f"Correlation (R):        {results['r_value']:.6f}")
        logger.info(f"P-value:                {results['p_value']:.2e}")
        logger.info(f"Std Error:              {results['std_err']:.8f}")
        logger.info(f"RMSE:                   {results['rmse']:.8f}")
        logger.info(f"N samples:              {results['n_samples']}")
    else:  # Multiple regression
        logger.info(f"Intercept:              {results['intercept']:.8f}")
        logger.info(f"Coef (Inflow):          {results['coef_inflow']:.8f} (p={results['p_values'][1]:.2e})")
        logger.info(f"Coef (Order Size):      {results['coef_order_size']:.8f} (p={results['p_values'][2]:.2e})")
        logger.info(f"R²:                     {results['r2']:.6f}")
        logger.info(f"RMSE:                   {results['rmse']:.8f}")
        logger.info(f"N samples:              {results['n_samples']}")
    
    logger.info("="*70 + "\n")

def analyze_correlations(inflows: np.ndarray, order_sizes: np.ndarray, slippages: np.ndarray):
    """
    Analyze correlations with multiple methods to detect linear and nonlinear relationships.
    """
    logger.info("\n### CORRELATION ANALYSIS ###\n")
    
    # --- Inflow vs Slippage ---
    logger.info("INFLOW vs SLIPPAGE:")
    pearson_r, pearson_p = stats.pearsonr(inflows, slippages)
    spearman_rho, spearman_p = stats.spearmanr(inflows, slippages)
    
    logger.info(f"  Pearson r:   {pearson_r:.6f} (p={pearson_p:.2e}) - LINEAR correlation")
    logger.info(f"  Spearman ρ:  {spearman_rho:.6f} (p={spearman_p:.2e}) - MONOTONIC correlation")
    
    if abs(spearman_rho) > abs(pearson_r) * 1.5:
        logger.info("  ⚠️  Spearman >> Pearson: suggests NONLINEAR but monotonic relationship")
    elif abs(spearman_rho) < 0.1 and abs(pearson_r) < 0.1:
        logger.info("  ⚠️  Both weak: likely no strong monotonic relationship")
    
    # --- Order Size vs Slippage ---
    logger.info("\nORDER SIZE vs SLIPPAGE:")
    pearson_r_size, pearson_p_size = stats.pearsonr(order_sizes, slippages)
    spearman_rho_size, spearman_p_size = stats.spearmanr(order_sizes, slippages)
    
    logger.info(f"  Pearson r:   {pearson_r_size:.6f} (p={pearson_p_size:.2e}) - LINEAR correlation")
    logger.info(f"  Spearman ρ:  {spearman_rho_size:.6f} (p={spearman_p_size:.2e}) - MONOTONIC correlation")
    
    if abs(spearman_rho_size) > abs(pearson_r_size) * 1.5:
        logger.info("  ⚠️  Spearman >> Pearson: suggests NONLINEAR but monotonic relationship")
    
    logger.info("")
    
    return {
        'inflow_pearson': pearson_r,
        'inflow_spearman': spearman_rho,
        'size_pearson': pearson_r_size,
        'size_spearman': spearman_rho_size
    }

def test_polynomial_fits(X: np.ndarray, y: np.ndarray, name: str = ""):
    """Test polynomial fits of different degrees."""
    logger.info(f"\nTESTING POLYNOMIAL FITS ({name}):")
    
    best_degree = 1
    best_r2 = 0
    results = {}
    
    for degree in [1, 2, 3]:
        coeffs = np.polyfit(X, y, degree)
        poly = np.poly1d(coeffs)
        y_pred = poly(X)
        
        # R-squared
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r2 = 1 - (ss_res / ss_tot)
        
        rmse = np.sqrt(np.mean((y - y_pred) ** 2))
        
        logger.info(f"  Degree {degree}: R² = {r2:.6f}, RMSE = {rmse:.6f}")
        
        results[degree] = {'r2': r2, 'rmse': rmse, 'poly': poly}
        
        if r2 > best_r2:
            best_r2 = r2
            best_degree = degree
    
    improvement = (results[best_degree]['r2'] - results[1]['r2']) / max(results[1]['r2'], 0.001) * 100
    logger.info(f"  → Best: Degree {best_degree}, improvement over linear: {improvement:.1f}%")
    
    return results

def test_logarithmic_fit(X: np.ndarray, y: np.ndarray, name: str = ""):
    """Test logarithmic relationship."""
    logger.info(f"\nTESTING LOGARITHMIC FIT ({name}):")
    
    # Log transform X (need positive values)
    X_clean = X[X > 0]
    y_clean = y[X > 0]
    
    if len(X_clean) < 2:
        logger.info("  Not enough positive X values")
        return None
    
    X_log = np.log(X_clean)
    slope, intercept, r_value, p_value, std_err = stats.linregress(X_log, y_clean)
    
    y_pred = slope * X_log + intercept
    rmse = np.sqrt(np.mean((y_clean - y_pred) ** 2))
    
    logger.info(f"  Logarithmic: R² = {r_value**2:.6f}, RMSE = {rmse:.6f}")
    logger.info(f"  Model: slippage = {slope:.6f} * log(inflow) + {intercept:.6f}")
    
    return {
        'r2': r_value ** 2,
        'rmse': rmse,
        'slope': slope,
        'intercept': intercept,
    }

def test_power_law_fit(X: np.ndarray, y: np.ndarray, name: str = ""):
    """Fit a power-law: slippage = C * order_size^alpha by regressing logs.
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

    n_total = len(X)
    n_used = len(Xp)
    n_excluded = n_total - n_used

    logger.info(f"  Samples used (positive X & y): {n_used:,}  Excluded: {n_excluded:,}")
    logger.info(f"  Power-law exponent (alpha): {alpha:.6f} (std_err={std_err:.6f}, p={p_value:.2e})")
    logger.info(f"  Prefactor (C): {prefactor:.6e}")
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
        'n_samples': n_used,
        'n_excluded': n_excluded,
        'X': Xp,
        'y': yp,
        'y_pred': y_pred,
        'logX': logX,
        'logY': logY,
        'y_pred_log': y_pred_log
    }

def plot_power_law_fit(order_sizes: np.ndarray, slippages: np.ndarray, fit_results: dict, filename: str = 'power_law_fit_ordersize_slippage.png'):
    """Plot scatter (log-log) and overlay power-law fit line."""
    if fit_results is None:
        logger.info("No power-law results to plot")
        return

    # Prepare arrays and show which points were used in the fit
    xs_all = np.array(order_sizes)
    ys_all = np.array(slippages)

    # Points used in the fit are returned in fit_results as 'X' and 'y'
    xs_used = np.array(fit_results.get('X', []))
    ys_used = np.array(fit_results.get('y', []))

    # Sample to avoid overplotting for background
    n_all = len(xs_all)
    if n_all > 50000:
        bg_idx = np.random.choice(n_all, 50000, replace=False)
        xs_bg = xs_all[bg_idx]
        ys_bg = ys_all[bg_idx]
    else:
        xs_bg = xs_all
        ys_bg = ys_all

    alpha = fit_results['alpha']
    C = fit_results['prefactor']

    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(12, 8))

    # Background scatter (all data, faint)
    ax.scatter(xs_bg, ys_bg, alpha=0.15, s=8, color='lightsteelblue', label='All settlements (background)')

    # Used points (darker)
    ax.scatter(xs_used, ys_used, alpha=0.35, s=10, color='steelblue', label='Samples used in fit')

    ax.set_xscale('log')
    ax.set_yscale('log')

    # Fit line over a range covering used X
    xmin = max(xs_used.min(), 1e-12)
    xmax = xs_used.max()
    x_line = np.logspace(np.log10(xmin), np.log10(xmax), 200)
    y_line = C * (x_line ** alpha)
    ax.plot(x_line, y_line, color='crimson', linewidth=2.5, label=f'Fit: y = {C:.3e} * x^{alpha:.4f}')

    # Annotate fit stats
    stat_text = f"alpha={alpha:.4f}, C={C:.3e}, R²={fit_results['r2']:.4f}, n={fit_results['n_samples']:,}"
    ax.text(0.02, 0.02, stat_text, transform=ax.transAxes, fontsize=10, verticalalignment='bottom', bbox=dict(facecolor='white', alpha=0.6))

    ax.set_xlabel('Order Size (shares)', fontsize=14, fontweight='bold')
    ax.set_ylabel('Slippage (USDC)', fontsize=14, fontweight='bold')
    ax.set_title('Power-law fit: Slippage vs Order Size', fontsize=16, fontweight='bold')
    ax.legend(fontsize=12)
    ax.grid(True, which='both', alpha=0.3)

    plt.tight_layout()
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    logger.info(f"✅ Power-law fit plot saved to {filename}")
    plt.close()


def analyze_and_cluster(order_sizes: np.ndarray, slippages: np.ndarray, settlement_data: list, n_clusters: int = 3):
    """Cluster (log order_size, log slippage) into `n_clusters` groups and produce diagnostic plots.

    Returns a dict with cluster assignments, GMM model, and summary statistics.
    """
    logger.info(f"\nCLUSTERING: fitting {n_clusters} components on positive order_size & slippage samples")
    # === Zorder configuration (modify here for layering) ===
    zorder_background = 1
    zorder_points = 50
    zorder_regression = 300
    # ======================================================
    logger.info("Note: `order_size` = max(making,taking)/SHARE_SCALAR (shares); `inflow` = min(making,taking)/USDC_SCALAR (USDC)")

    xs = np.array(order_sizes)
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
            'median_order_size': median_size,
            'median_slippage': median_slip,
            'top_maker_assets': top_maker,
            'top_taker_assets': top_taker,
            'example_settlements': cluster_settlements[:3]
        })

    # --- Plot 1: cluster density visualization (log-log) ---
    plt.rcParams.update({'font.size': 20})
    fig, ax = plt.subplots(figsize=(14, 10))
    colors = sns.color_palette('deep', n_clusters)

    # Plot all points faint as background (including zeros) but don't add legend entry
    ax.scatter(xs, ys, s=8, alpha=0.03, color='lightgray', zorder=zorder_background)

    # Plot clustered positive points colored
    masked_orig_idx = np.nonzero(mask_pos)[0]
    for k in range(n_clusters):
        sel = (labels == k)
        ax.scatter(xs[masked_orig_idx[sel]], ys[masked_orig_idx[sel]], s=12, alpha=0.75, color=colors[k], label=f'Cluster {k} (n={cluster_summary[k]["count"]:,})', edgecolors='none', zorder=zorder_points)

    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.set_xlabel('Order Size (shares)', fontsize=26, fontweight='bold')
    ax.set_ylabel('Slippage (USDC)', fontsize=26, fontweight='bold')
    ax.tick_params(axis='both', which='major', labelsize=20)
    ax.set_title(f'Clustered Slippage / Ordersize (n={n_clusters})', fontsize=28, fontweight='bold')

    # Legend with larger colored texts (no marker/handle on the left)
    from matplotlib.lines import Line2D
    labels = [f'Cluster {k} (n={cluster_summary[k]["count"]:,})' for k in range(n_clusters)]
    proxy_handles = [Line2D([], [], linestyle='', marker='', color='none') for _ in range(n_clusters)]
    leg = ax.legend(proxy_handles, labels, loc='upper left', bbox_to_anchor=(0.01, 0.99), prop={'size': 20}, handlelength=0, handletextpad=0.4)
    leg.get_frame().set_alpha(0.92)
    for i, text in enumerate(leg.get_texts()):
        text.set_color(colors[i])
        text.set_fontsize(20)

    plt.tight_layout()
    cluster_plot_file = 'clusters_density_loglog.png'
    plt.savefig(cluster_plot_file, dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"✅ Cluster density plot saved to {cluster_plot_file}")

    # --- Plot 2: classification summary ---
    fig, ax = plt.subplots(figsize=(10, 6))
    counts = [c['count'] for c in cluster_summary]
    ax.bar(range(n_clusters), counts, color=colors)
    ax.set_xlabel('Cluster', fontsize=18)
    ax.set_ylabel('Count', fontsize=18)
    ax.set_title('Cluster counts', fontsize=20)
    ax.tick_params(axis='both', labelsize=16)
    for i, v in enumerate(counts):
        ax.text(i, v + max(counts) * 0.01, f'{v:,}', ha='center', fontsize=14)

    stats_text = f"GMM BIC={bic:.1f}, AIC={aic:.1f}, Silhouette={sil_score if sil_score is not None else 'NA'}"
    ax.text(0.02, 0.95, stats_text, transform=ax.transAxes, fontsize=12, verticalalignment='top', bbox=dict(facecolor='white', alpha=0.7))
    class_plot_file = 'cluster_classification_summary.png'
    plt.tight_layout()
    plt.savefig(class_plot_file, dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"✅ Cluster summary plot saved to {class_plot_file}")

    # Save CSV summary
    import csv
    csv_file = 'cluster_summary.csv'
    with open(csv_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['cluster', 'count', 'median_order_size', 'median_slippage', 'top_maker_assets', 'top_taker_assets'])
        for s in cluster_summary:
            writer.writerow([s['cluster'], s['count'], s['median_order_size'], s['median_slippage'], str(s['top_maker_assets']), str(s['top_taker_assets'])])
    logger.info(f"✅ Cluster CSV summary saved to {csv_file}")

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

    # Plot per-cluster fits overlay
    fig, ax = plt.subplots(figsize=(12, 9))
    ax.set_xscale('log')
    ax.set_yscale('log')
    ax.scatter(xs, ys, s=8, alpha=0.03, color='lightgray', zorder=zorder_background)
    for k in range(n_clusters):
        sel = (labels == k)
        ax.scatter(xs[masked_orig_idx[sel]], ys[masked_orig_idx[sel]], s=12, alpha=0.75, color=colors[k], label=f'Cluster {k} (n={cluster_summary[k]["count"]:,})', edgecolors='none', zorder=zorder_points)
        # Only draw regression lines for clusters 0 and 2
        if k in [0, 2] and k in cluster_fits and cluster_fits[k] is not None:
            Ck = cluster_fits[k]['prefactor']
            alphak = cluster_fits[k]['alpha']
            xmin = max(xs[masked_orig_idx[sel]].min(), 1e-12)
            xmax = xs[masked_orig_idx[sel]].max()
            x_line = np.logspace(np.log10(xmin), np.log10(xmax), 200)
            # Solid black regression lines, drawn on top
            ax.plot(x_line, Ck * (x_line ** alphak), color='black', linewidth=6.0, linestyle='-', zorder=zorder_regression)

    ax.set_xlabel('Order Size (shares)', fontsize=22, fontweight='bold')
    ax.set_ylabel('Slippage (USDC)', fontsize=22, fontweight='bold')
    ax.tick_params(axis='both', which='major', labelsize=18)
    ax.set_title('Cluster-specific power-law fits (log-log)', fontsize=24, fontweight='bold')
    # Create legend without marker handles and larger colored labels
    from matplotlib.lines import Line2D
    labels2 = [f'Cluster {k} (n={cluster_summary[k]["count"]:,})' for k in range(n_clusters)]
    proxy2 = [Line2D([], [], linestyle='', marker='', color='none') for _ in range(n_clusters)]
    leg2 = ax.legend(proxy2, labels2, loc='upper left', bbox_to_anchor=(0.01, 0.99), prop={'size': 18}, handlelength=0, handletextpad=0.4)
    leg2.get_frame().set_alpha(0.92)
    for i, text in enumerate(leg2.get_texts()):
        text.set_color(colors[i])
        text.set_fontsize(18)

    plt.tight_layout()
    per_cluster_fit_file = 'cluster_powerlaw_fits.png'
    plt.savefig(per_cluster_fit_file, dpi=300, bbox_inches='tight')
    plt.close()
    logger.info(f"✅ Cluster power-law fits plot saved to {per_cluster_fit_file}")

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

def plot_slippage_vs_ordersize(order_sizes: np.ndarray, slippages: np.ndarray, results: dict):
    """Plot slippage vs order size with regression line."""
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, ax = plt.subplots(figsize=(14, 9))
    
    # Scatter plot (sample to avoid overplotting)
    sample_idx = np.random.choice(len(order_sizes), min(50000, len(order_sizes)), replace=False)
    ax.scatter(order_sizes[sample_idx], slippages[sample_idx], alpha=0.3, s=10, color='steelblue', label='Settlements')
    
    # Regression line
    X_sorted = np.sort(order_sizes)
    y_pred_sorted = results['slope'] * X_sorted + results['intercept']
    ax.plot(X_sorted, y_pred_sorted, 'r-', linewidth=3, 
           label=f"Linear fit: slippage = {results['slope']:.6f} * size + {results['intercept']:.4f}")
    
    ax.set_xlabel('Order Size (Shares)', fontsize=18, fontweight='bold')
    ax.set_ylabel('Slippage (USDC)', fontsize=18, fontweight='bold')
    ax.set_title(f'Slippage vs Order Size (R² = {results["r_squared"]:.4f})', fontsize=22, fontweight='bold')
    ax.legend(fontsize=16, loc='upper right', framealpha=0.95)
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    filename = 'slip_vs_ordersize.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    logger.info(f"✅ Plot saved to {filename}")
    plt.close()

def plot_stratified_analysis(df: pd.DataFrame):
    """Create stratified analysis by order size buckets."""
    # Create order size buckets
    df['size_bucket'] = pd.qcut(df['order_size'], q=5, labels=['XS', 'S', 'M', 'L', 'XL'], duplicates='drop')
    
    plt.style.use('seaborn-v0_8-whitegrid')
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    
    # 1. Box plot: slippage by size bucket
    ax = axes[0, 0]
    df.boxplot(column='slippage', by='size_bucket', ax=ax)
    ax.set_xlabel('Order Size Bucket', fontsize=14, fontweight='bold')
    ax.set_ylabel('Slippage (USDC)', fontsize=14, fontweight='bold')
    ax.set_title('Slippage Distribution by Order Size', fontsize=16, fontweight='bold')
    ax.get_figure().suptitle('')  # Remove automatic title
    
    # 2. Slippage/Inflow ratio by bucket
    ax = axes[0, 1]
    df['slip_per_dollar'] = df['slippage'] / df['inflow']
    bucket_stats = df.groupby('size_bucket')['slip_per_dollar'].agg(['mean', 'std', 'count'])
    ax.bar(range(len(bucket_stats)), bucket_stats['mean'], yerr=bucket_stats['std'], capsize=5, alpha=0.7)
    ax.set_xlabel('Order Size Bucket', fontsize=14, fontweight='bold')
    ax.set_ylabel('Mean Slippage / Inflow ($)', fontsize=14, fontweight='bold')
    ax.set_title('Slippage Cost per Dollar by Order Size', fontsize=16, fontweight='bold')
    ax.set_xticklabels(bucket_stats.index)
    ax.grid(True, alpha=0.3, axis='y')
    
    # 3. Count by bucket
    ax = axes[1, 0]
    bucket_counts = df['size_bucket'].value_counts().sort_index()
    ax.bar(range(len(bucket_counts)), bucket_counts.values, alpha=0.7, color='coral')
    ax.set_xlabel('Order Size Bucket', fontsize=14, fontweight='bold')
    ax.set_ylabel('Count', fontsize=14, fontweight='bold')
    ax.set_title('Number of Settlements by Order Size', fontsize=16, fontweight='bold')
    ax.set_xticklabels(bucket_counts.index)
    ax.grid(True, alpha=0.3, axis='y')
    
    # 4. Average inflow by bucket
    ax = axes[1, 1]
    bucket_inflow = df.groupby('size_bucket')['inflow'].mean()
    ax.bar(range(len(bucket_inflow)), bucket_inflow.values, alpha=0.7, color='green')
    ax.set_xlabel('Order Size Bucket', fontsize=14, fontweight='bold')
    ax.set_ylabel('Average Inflow ($)', fontsize=14, fontweight='bold')
    ax.set_title('Average USDC Inflow by Order Size', fontsize=16, fontweight='bold')
    ax.set_xticklabels(bucket_inflow.index)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    filename = 'slip_stratified_analysis.png'
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    logger.info(f"✅ Stratified analysis saved to {filename}")
    plt.close()

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
        
        # --- ZERO-SLIPPAGE DIAGNOSTICS ---
        zero_mask = slippages_arr == 0
        n_zero = int(np.sum(zero_mask))
        pct_zero = (n_zero / len(slippages_arr)) * 100 if len(slippages_arr) > 0 else 0.0
        max_taker_zero = float(inflows_arr[zero_mask].max()) if n_zero > 0 else 0.0
        logger.info("\nZERO-SLIPPAGE DIAGNOSTICS:")
        logger.info(f"  Count with slippage == 0: {n_zero:,} ({pct_zero:.6f}%)")
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
        # --- SIMPLE REGRESSIONS ---
        logger.info("\n### SIMPLE REGRESSION ANALYSES ###\n")
        
        # Slippage vs Inflow
        results_inflow = perform_simple_regression(inflows_arr, slippages_arr)
        log_regression_summary(results_inflow, "REGRESSION 1: Slippage vs Inflow")
        
        # Slippage vs Order Size
        results_size = perform_simple_regression(order_sizes_arr, slippages_arr)
        log_regression_summary(results_size, "REGRESSION 2: Slippage vs Order Size")
        
        # --- MULTIPLE REGRESSION ---
        logger.info("\n### MULTIPLE REGRESSION ###\n")
        results_multiple = perform_regression(list(inflows_arr), list(order_sizes_arr), list(slippages_arr))
        log_regression_summary(results_multiple, "REGRESSION 3: Slippage vs Inflow + Order Size")
        
        # --- CORRELATION ANALYSIS (LINEAR vs NONLINEAR) ---
        corr_results = analyze_correlations(inflows_arr, order_sizes_arr, slippages_arr)
        
        # Test for nonlinear relationships
        poly_results_inflow = test_polynomial_fits(inflows_arr, slippages_arr, "Inflow vs Slippage")
        poly_results_size = test_polynomial_fits(order_sizes_arr, slippages_arr, "Order Size vs Slippage")
        
        log_results = test_logarithmic_fit(inflows_arr, slippages_arr, "Inflow vs Slippage")
        
        # Power-law fit (order size vs slippage)
        power_results = test_power_law_fit(order_sizes_arr, slippages_arr, "Order Size vs Slippage")
        plot_power_law_fit(order_sizes_arr, slippages_arr, power_results)
        
        # --- CLUSTERING ANALYSIS ---
        cluster_results = analyze_and_cluster(order_sizes_arr, slippages_arr, filtered_settlement_data, n_clusters=3)
        
        # --- PLOTS & STRATIFIED ANALYSIS ---
        plot_slippage_vs_ordersize(order_sizes_arr, slippages_arr, results_size)
        
        df = pd.DataFrame({
            'inflow': inflows_arr,
            'order_size': order_sizes_arr,
            'slippage': slippages_arr
        })
        plot_stratified_analysis(df)
        
        # Compute and log comprehensive statistics
        stats_dict = compute_statistics(inflows_arr, slippages_arr)
        log_statistics_summary(stats_dict)
        
        # Summary statistics
        logger.info("\n### SUMMARY STATISTICS ###\n")
        logger.info(f"Inflow:      mean=${inflows_arr.mean():.2f}, median=${np.median(inflows_arr):.2f}, std=${inflows_arr.std():.2f}")
        logger.info(f"Order Size:  mean={order_sizes_arr.mean():.2f} shares, median={np.median(order_sizes_arr):.2f}, std={order_sizes_arr.std():.2f}")
        logger.info(f"Slippage:    mean=${slippages_arr.mean():.4f}, median=${np.median(slippages_arr):.4f}, std=${slippages_arr.std():.4f}")
        logger.info("")
        
    finally:
        if conn:
            await conn.close()

if __name__ == "__main__":
    if not all([DB_USER, DB_PASS, DB_NAME, PG_HOST, PG_PORT]):
        logger.error("Database credentials not set.")
    else:
        asyncio.run(main())
