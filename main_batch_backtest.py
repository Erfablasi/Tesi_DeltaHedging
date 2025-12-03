import pandas as pd
import numpy as np
import time
import os

from src.data_loaders import VolatilityManager, RatesManager, DividendsManager
from src.strategy import WhalleyHedgingStrategy 
from src.models import pbs_delta, pbs_gamma, pbs_price 

# CONFIGURAZIONE MONEYNESS
MONEYNESS_LEVELS = {
    'ITM': 0.95,
    'ATM': 1.00,
    'OTM': 1.05
}

# CONFIGURAZIONE CATEGORIE TEMPORALI (Giorni)
TERM_THRESHOLDS = {
    'Breve_Termine': 90,     # Fino a 3 mesi
    'Medio_Termine': 180,    # Fino a 6 mesi
    'Lungo_Termine': 9999    # Oltre 6 mesi
}

def ensure_dir(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def get_term_category(days_to_expiry):
    if days_to_expiry <= TERM_THRESHOLDS['Breve_Termine']:
        return 'Breve_Termine'
    elif days_to_expiry <= TERM_THRESHOLDS['Medio_Termine']:
        return 'Medio_Termine'
    else:
        return 'Lungo_Termine'

def run_single_simulation(vol_engine, rates_engine, div_engine, df_spot, 
                          expiry_date, category, initial_spot, moneyness_label):
    
    # 1. Calcolo Strike
    target_strike_raw = initial_spot * MONEYNESS_LEVELS[moneyness_label]
    TARGET_STRIKE = round(target_strike_raw / 50) * 50 
    
    # 2. Setup Periodo
    # La simulazione parte dalla data globale di inizio dati (o poco dopo)
    start_date = max(df_spot['AsOfDate'].min(), vol_engine.df.index.levels[0].min())
    
    # IMPORTANTE: Se la scadenza è già passata rispetto all'inizio dati, saltiamo
    if expiry_date <= start_date:
        return

    # Filtro Dati
    df_sim = df_spot[(df_spot['AsOfDate'] >= start_date) & (df_spot['AsOfDate'] <= expiry_date)]
    
    if len(df_sim) == 0:
        return

    # 3. Setup Strategia
    whalley_strat = WhalleyHedgingStrategy(risk_aversion=1.0, transaction_cost=0.002)
    
    # 4. Loop Trading
    for row in df_sim.itertuples():
        now = row.AsOfDate
        spot = row.Spot
        today_date = pd.Timestamp(now.date()) 
        
        T = (expiry_date - now).total_seconds() / (365.25 * 24 * 3600)
        if T <= 0.0001: break

        q = div_engine.get_yield_q(today_date)
        tau_days = T * 365.25
        r = rates_engine.get_risk_free_rate(today_date, tau_days)
        
        iv = vol_engine.get_interpolated_iv(today_date, expiry_date, TARGET_STRIKE)
        
        if iv is not None and iv > 0:
            spot_adj = spot * np.exp(-q * T)
            delta = pbs_delta(spot_adj, TARGET_STRIKE, T, r, 0, iv)
            gamma = pbs_gamma(spot_adj, TARGET_STRIKE, T, r, 0, iv)
            opt_price = pbs_price(spot_adj, TARGET_STRIKE, T, r, 0, iv)
            
            whalley_strat.rebalance(now, spot, T, r, delta, gamma, opt_price)

    # 5. Salvataggio
    output_dir = os.path.join("results", category)
    ensure_dir(output_dir)
    
    filename = f"{moneyness_label}_{expiry_date.date()}.csv"
    full_path = os.path.join(output_dir, filename)
    
    res = whalley_strat.get_log_dataframe()
    if not res.empty:
        res.to_csv(full_path, index=False)
        print(f"       [OK] Salvato: {full_path} ({len(res)} ticks)")

def run_batch_backtest():
    start_time = time.time()
    print(f"--- AVVIO BATCH BACKTEST WHALLEY (AUTO-DISCOVERY) ---")

    # 1. Caricamento Motori
    try:
        vol_path = "data/iv_surface_empirical_anchored.parquet"
        print(f"Lettura superficie per estrazione scadenze: {vol_path}")
        vol_engine = VolatilityManager(vol_path)
        
        rates_engine = RatesManager("data/daily_rates_linear_smoothed_long.parquet")
        div_engine = DividendsManager("data/dividends.parquet")
    except FileNotFoundError as e:
        print(f"ERRORE FATALE MOTORI: {e}")
        return

    print("Caricamento Spot Prices...")
    df_spot = pd.read_parquet("data/spot_prices_min.parquet")
    df_spot['AsOfDate'] = pd.to_datetime(df_spot['AsOfDate'])
    df_spot = df_spot.sort_values("AsOfDate")
    
    # 2. ESTERAZIONE SCADENZE DAL PARQUET
    # L'indice del vol_engine è MultiIndex (AsOfDate, expiry_date)
    # Prendiamo tutte le expiry_date uniche presenti nel livello 1 dell'indice
    all_expiries = vol_engine.df.index.get_level_values('expiry_date').unique().sort_values()
    
    # Data Inizio Simulazione (Intersezione dati spot e vol)
    global_start_date = max(df_spot['AsOfDate'].min(), vol_engine.df.index.levels[0].min())
    print(f"Inizio Dati Disponibili: {global_start_date.date()}")
    
    # Filtriamo solo le scadenze future rispetto all'inizio dati
    future_expiries = [e for e in all_expiries if e > global_start_date]
    print(f"Trovate {len(future_expiries)} scadenze future da processare.")

    # 3. CICLO SULLE SCADENZE
    for i, expiry in enumerate(future_expiries):
        # Calcolo Giorni alla scadenza (rispetto all'inizio simulazione)
        days_to_expiry = (expiry - global_start_date).days
        category = get_term_category(days_to_expiry)
        
        print(f"\n[{i+1}/{len(future_expiries)}] Processing: {expiry.date()} (Tau: {days_to_expiry}gg -> {category})")
        
        # Determina Spot Iniziale
        try:
            # Troviamo lo spot nel primo momento utile della simulazione
            initial_spot_row = df_spot[df_spot['AsOfDate'] >= global_start_date].iloc[0]
            initial_spot = initial_spot_row['Spot']
        except IndexError:
            print("   [ERR] Impossibile trovare spot iniziale.")
            continue

        # Lancia le 3 Moneyness
        for moneyness in ['ITM', 'ATM', 'OTM']:
            run_single_simulation(
                vol_engine, rates_engine, div_engine, df_spot,
                expiry_date=expiry,
                category=category,
                initial_spot=initial_spot,
                moneyness_label=moneyness
            )

    elapsed = time.time() - start_time
    print(f"\n--- BATCH COMPLETO in {elapsed:.2f}s ---")
    print("Report generati in 'results/Breve_Termine', 'results/Medio_Termine', ecc.")

if __name__ == "__main__":
    run_batch_backtest()