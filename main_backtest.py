import pandas as pd
import numpy as np
import time

from src.data_loaders import VolatilityManager, RatesManager, DividendsManager
from src.strategy import WhalleyHedgingStrategy 
# MODIFICATO: Importiamo pbs_price
from src.models import pbs_delta, pbs_gamma, pbs_price 

def run_backtest():
    start_time = time.time()
    print(f"--- AVVIO BACKTEST WHALLEY (ANALYSIS READY) ---")

    # 1. CARICAMENTO DATI
    vol_path = "data/iv_surface_empirical_anchored.parquet"
    rates_path = "data/daily_rates_linear_smoothed_long.parquet"
    div_path = "data/dividends.parquet"
    spot_path = "data/spot_prices_min.parquet"

    try:
        vol_engine = VolatilityManager(vol_path)
        rates_engine = RatesManager(rates_path)
        div_engine = DividendsManager(div_path)
    except FileNotFoundError as e:
        print(f"ERRORE FATALE: {e}")
        return

    print("Caricamento Spot Prices...")
    df_spot = pd.read_parquet(spot_path)
    df_spot['AsOfDate'] = pd.to_datetime(df_spot['AsOfDate'])
    df_spot = df_spot.sort_values("AsOfDate")
    
    # 2. PERIODO
    spot_start = df_spot['AsOfDate'].min()
    vol_start = vol_engine.df.index.levels[0].min()
    sim_start_date = max(spot_start, vol_start)
    
    # 3. SCADENZA
    available_expiries = vol_engine.df.index.levels[1].unique()
    valid_expiries = [e for e in available_expiries if e > sim_start_date + pd.Timedelta(days=45)]
    
    if not valid_expiries:
        TARGET_EXPIRY = available_expiries[-1]
    else:
        TARGET_EXPIRY = valid_expiries[0]

    # 4. STRIKE
    initial_spot_row = df_spot[df_spot['AsOfDate'] >= sim_start_date].iloc[0]
    initial_spot = initial_spot_row['Spot'] 
    TARGET_STRIKE = round(initial_spot / 50) * 50
    
    print(f"\nSTART: {sim_start_date.date()} | EXPIRY: {TARGET_EXPIRY.date()} | STRIKE: {TARGET_STRIKE}")

    # 5. FILTRO
    df_sim = df_spot[(df_spot['AsOfDate'] >= sim_start_date) & (df_spot['AsOfDate'] <= TARGET_EXPIRY)]
    print(f"Ticks da processare: {len(df_sim)}")
    
    whalley_strat = WhalleyHedgingStrategy(risk_aversion=1.0, transaction_cost=0.002)

    # 6. LOOP
    for row in df_sim.itertuples():
        now = row.AsOfDate
        spot = row.Spot
        today_date = pd.Timestamp(now.date()) 
        
        T = (TARGET_EXPIRY - now).total_seconds() / (365.25 * 24 * 3600)
        if T <= 0.0001: break

        q = div_engine.get_yield_q(today_date)
        tau_days = T * 365.25
        r = rates_engine.get_risk_free_rate(today_date, tau_days)
        
        iv = vol_engine.get_interpolated_iv(today_date, TARGET_EXPIRY, TARGET_STRIKE)
        
        if iv is not None and iv > 0:
            spot_adj = spot * np.exp(-q * T)
            
            delta = pbs_delta(spot_adj, TARGET_STRIKE, T, r, 0, iv)
            gamma = pbs_gamma(spot_adj, TARGET_STRIKE, T, r, 0, iv)
            
            # NUOVO: Calcoliamo il prezzo
            opt_price = pbs_price(spot_adj, TARGET_STRIKE, T, r, 0, iv)
            
            # NUOVO: Passiamo opt_price alla funzione rebalance
            whalley_strat.rebalance(
                timestamp=now, 
                S=spot, 
                T_rem=T, 
                r=r, 
                Delta_PBS=delta, 
                Gamma_PBS=gamma, 
                Option_Value=opt_price
            )

    # 7. SALVATAGGIO
    res = whalley_strat.get_log_dataframe()
    fname = "results_whalley.csv" 
    res.to_csv(fname, index=False)
    
    elapsed = time.time() - start_time
    print(f"\nCompletato in {elapsed:.2f}s.")
    print(f"Salvato in: {fname} ({len(res)} righe)")

if __name__ == "__main__":
    run_backtest()