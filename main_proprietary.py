import pandas as pd
import numpy as np
import time
import os

# 1. IMPORT STANDARD (Ora funzionano perché siamo nella root)
from src.data_loaders import VolatilityManager, RatesManager, DividendsManager
from src.models import pbs_delta, pbs_gamma, pbs_price 

# 2. IMPORT STRATEGIA PROPRIETARIA
# Python vede la cartella 'proprietary_strat' come un pacchetto
from proprietary_strat.src.strategy_custom import AdaptiveLossStrategy

# CONFIGURAZIONI
MONEYNESS_LEVELS = {'ITM': 0.95, 'ATM': 1.00, 'OTM': 1.05}
TERM_THRESHOLDS = {'Breve_Termine': 90, 'Medio_Termine': 180, 'Lungo_Termine': 9999}

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def get_term_category(days_to_expiry):
    if days_to_expiry <= TERM_THRESHOLDS['Breve_Termine']: return 'Breve_Termine'
    elif days_to_expiry <= TERM_THRESHOLDS['Medio_Termine']: return 'Medio_Termine'
    else: return 'Lungo_Termine'

def run_single_simulation(vol_engine, rates_engine, div_engine, df_spot, 
                          expiry_date, category, initial_spot, moneyness_label):
    
    # Setup
    target_strike_raw = initial_spot * MONEYNESS_LEVELS[moneyness_label]
    TARGET_STRIKE = round(target_strike_raw / 50) * 50 
    
    start_date = max(df_spot['AsOfDate'].min(), vol_engine.df.index.levels[0].min())
    if expiry_date <= start_date: return

    df_sim = df_spot[(df_spot['AsOfDate'] >= start_date) & (df_spot['AsOfDate'] <= expiry_date)]
    if len(df_sim) == 0: return

    # --- SETUP CUSTOM STRATEGY ---
    custom_strat = AdaptiveLossStrategy(risk_aversion_weight=0.5, transaction_cost=0.002)
    
    prev_time = None
    
    for row in df_sim.itertuples():
        now = row.AsOfDate
        spot = row.Spot
        today_date = pd.Timestamp(now.date()) 
        
        T = (expiry_date - now).total_seconds() / (365.25 * 24 * 3600)
        if T <= 0.0001: break
        
        if prev_time is None: dt_hours = 1.0/60.0 
        else: dt_hours = (now - prev_time).total_seconds() / 3600.0
        prev_time = now

        q = div_engine.get_yield_q(today_date)
        tau_days = T * 365.25
        r = rates_engine.get_risk_free_rate(today_date, tau_days)
        iv = vol_engine.get_interpolated_iv(today_date, expiry_date, TARGET_STRIKE)
        
        if iv is not None and iv > 0:
            spot_adj = spot * np.exp(-q * T)
            delta = pbs_delta(spot_adj, TARGET_STRIKE, T, r, 0, iv)
            gamma = pbs_gamma(spot_adj, TARGET_STRIKE, T, r, 0, iv)
            opt_price = pbs_price(spot_adj, TARGET_STRIKE, T, r, 0, iv)
            
            custom_strat.rebalance(
                timestamp=now, S=spot, T_rem=T, r=r, 
                Delta_PBS=delta, Gamma_PBS=gamma, 
                Volatility_IV=iv, dt_hours=dt_hours, 
                Option_Value=opt_price
            )

    # SALVATAGGIO NELLA CARTELLA SPECIFICA PROPRIETARY
    # Percorso: proprietary_strat/results/Breve_Termine/...
    output_dir = os.path.join("proprietary_strat", "results", category)
    ensure_dir(output_dir)
    
    filename = f"CUSTOM_{moneyness_label}_{expiry_date.date()}.csv"
    full_path = os.path.join(output_dir, filename)
    
    res = custom_strat.get_log_dataframe()
    if not res.empty:
        res.to_csv(full_path, index=False)
        print(f"       [OK] Salvato: {filename}")

def run_batch_proprietary():
    start_time = time.time()
    print(f"--- AVVIO BATCH: PROPRIETARY STRATEGY ---")

    try:
        # Percorsi semplici relativi alla root
        vol_engine = VolatilityManager("data/iv_surface_empirical_anchored.parquet")
        rates_engine = RatesManager("data/daily_rates_linear_smoothed_long.parquet")
        div_engine = DividendsManager("data/dividends.parquet")
    except FileNotFoundError as e:
        print(f"ERRORE: {e}")
        return

    print("Caricamento Spot...")
    df_spot = pd.read_parquet("data/spot_prices_min.parquet")
    df_spot['AsOfDate'] = pd.to_datetime(df_spot['AsOfDate'])
    df_spot = df_spot.sort_values("AsOfDate")
    
    # Discovery
    all_expiries = vol_engine.df.index.get_level_values('expiry_date').unique().sort_values()
    global_start = max(df_spot['AsOfDate'].min(), vol_engine.df.index.levels[0].min())
    future_expiries = [e for e in all_expiries if e > global_start]
    
    print(f"Trovate {len(future_expiries)} scadenze future.")

    for i, expiry in enumerate(future_expiries):
        days_to_expiry = (expiry - global_start).days
        category = get_term_category(days_to_expiry)
        
        print(f"\n[{i+1}/{len(future_expiries)}] {expiry.date()} ({category})")
        
        try:
            initial_spot_row = df_spot[df_spot['AsOfDate'] >= global_start].iloc[0]
            try: initial_spot = initial_spot_row['Spot']
            except KeyError: initial_spot = initial_spot_row['spot']
        except: continue

        for moneyness in ['ITM', 'ATM', 'OTM']:
            run_single_simulation(
                vol_engine, rates_engine, div_engine, df_spot,
                expiry, category, initial_spot, moneyness
            )
            
    elapsed = time.time() - start_time
    print(f"\n--- BATCH PROPRIETARIO COMPLETATO ({elapsed:.2f}s) ---")

if __name__ == "__main__":
    run_batch_proprietary()