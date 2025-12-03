import pandas as pd
import numpy as np
import os
import glob

def calculate_kpi(df, strategy_name, category, moneyness, expiry):
    """
    Calcola le metriche finanziarie per una singola simulazione.
    """
    # 1. Ricostruzione P&L (uguale al tuo codice)
    # Hedge Portfolio = Cash + (Shares * Spot)
    # Total PnL = Hedge Portfolio - Option Value
    # Assumiamo Short Call iniziale
    
    # Verifica colonne necessarie
    required_cols = ['Cash', 'Held_Shares', 'Spot', 'Option_Value', 'Transaction_Cost']
    if not all(col in df.columns for col in required_cols):
        return None

    df['Hedge_Portfolio'] = df['Cash'] + (df['Held_Shares'] * df['Spot'])
    df['Total_PnL'] = df['Hedge_Portfolio'] - df['Option_Value']
    
    # Normalizzazione PnL (parte da 0)
    start_pnl = df['Total_PnL'].iloc[0]
    df['Rel_PnL'] = df['Total_PnL'] - start_pnl
    
    # 2. Calcolo KPI
    total_costs = df['Transaction_Cost'].sum()
    
    # Conta i trade (dove Action non è HOLD)
    n_trades = df[df['Action'] != 'HOLD'].shape[0]
    total_ticks = len(df)
    trade_freq_pct = (n_trades / total_ticks) * 100 if total_ticks > 0 else 0
    
    final_pnl = df['Rel_PnL'].iloc[-1]
    
    # Volatilità P&L (Tick-by-tick)
    pnl_volatility = df['Rel_PnL'].diff().std()
    
    # Tracking Error del Delta (Media deviazione assoluta dal delta ideale)
    # Utile per vedere quanto la strategia "lascia correre" il rischio
    if 'Ideal_Delta' in df.columns:
        delta_mae = (df['Held_Shares'] - df['Ideal_Delta']).abs().mean()
    else:
        delta_mae = 0.0

    return {
        'Strategy': strategy_name,
        'Category': category,
        'Moneyness': moneyness,
        'Expiry': expiry,
        'Total_Costs_EUR': total_costs,
        'Final_PnL_EUR': final_pnl,
        'PnL_Volatility': pnl_volatility,
        'Num_Trades': n_trades,
        'Trade_Freq_Pct': trade_freq_pct,
        'Delta_Tracking_Error': delta_mae,
        'Ticks': total_ticks
    }

def run_comprehensive_analysis():
    print("--- AVVIO ANALISI COMPARATIVA MASSIVA ---")
    
    all_results = []
    
    # 1. DEFINIZIONE PERCORSI DA SCANSIONARE
    # Struttura: (Nome Strategia, Percorso Cartella Root dei risultati)
    paths_to_scan = [
        ("Whalley", "results"),
        ("Custom_Adaptive", "proprietary_strat/results")
    ]
    
    for strat_name, root_folder in paths_to_scan:
        if not os.path.exists(root_folder):
            print(f"Warning: Cartella {root_folder} non trovata. Salto.")
            continue
            
        print(f"\nScansione Strategia: {strat_name} in '{root_folder}'...")
        
        # Cerca tutti i CSV ricorsivamente
        # Pattern atteso: root_folder / Categoria / Moneyness_Expiry.csv
        files = glob.glob(os.path.join(root_folder, "**", "*.csv"), recursive=True)
        
        for file_path in files:
            # Parsing del percorso per estrarre metadati
            # Esempio: results/Breve_Termine/ITM_2024-11-15.csv
            try:
                # Cartella padre = Categoria (es. Breve_Termine)
                category = os.path.basename(os.path.dirname(file_path))
                
                # Nome file = Moneyness_Expiry.csv
                filename = os.path.basename(file_path)
                name_parts = filename.replace(".csv", "").split("_")
                
                # Gestione nomi file: ITM_2024-11-15 (Whalley) vs CUSTOM_ITM_2024-11-15 (Custom)
                if strat_name == "Custom_Adaptive" and name_parts[0] == "CUSTOM":
                    moneyness = name_parts[1]
                    expiry = name_parts[2]
                else:
                    moneyness = name_parts[0]
                    expiry = name_parts[1]
                
                # Carica e Calcola
                df = pd.read_csv(file_path)
                kpi = calculate_kpi(df, strat_name, category, moneyness, expiry)
                
                if kpi:
                    all_results.append(kpi)
                    # print(f"   -> OK: {category} - {moneyness}")
                
            except Exception as e:
                print(f"   -> ERRORE file {file_path}: {e}")

    # 2. CREAZIONE DATAFRAME FINALE
    if not all_results:
        print("\nNessun risultato valido trovato!")
        return

    df_final = pd.DataFrame(all_results)
    
    # Ordina per pulizia
    df_final = df_final.sort_values(by=['Category', 'Moneyness', 'Expiry', 'Strategy'])
    
    # 3. SALVATAGGIO REPORT
    output_filename = "FINAL_COMPARISON_REPORT.csv"
    df_final.to_csv(output_filename, index=False)
    
    print(f"\n--- ANALISI COMPLETATA ---")
    print(f"Report salvato in: {output_filename}")
    print(f"Totale Simulazioni Analizzate: {len(df_final)}")
    
    # 4. ANTEPRIMA AGGREGATA (Pivot Table per la Tesi)
    print("\n--- ANTEPRIMA MEDIE (Strategy vs Category) ---")
    pivot = df_final.pivot_table(
        index=['Category', 'Strategy'], 
        values=['Total_Costs_EUR', 'PnL_Volatility', 'Final_PnL_EUR'], 
        aggfunc='mean'
    )
    print(pivot)

if __name__ == "__main__":
    run_comprehensive_analysis()