import pandas as pd
import numpy as np

def run_analysis():
    filename = "results_whalley.csv"
    print(f"--- ANALISI RISULTATI: {filename} ---")
    
    try:
        df = pd.read_csv(filename)
    except FileNotFoundError:
        print("Errore: Esegui prima main_backtest.py!")
        return

    # 1. Ricostruzione P&L
    # Portfolio = Cash + Azioni - Debito Opzione (Short Call)
    df['Hedge_Portfolio'] = df['Cash'] + (df['Held_Shares'] * df['Spot'])
    df['Total_PnL'] = df['Hedge_Portfolio'] - df['Option_Value']
    
    # PnL relativo (parte da 0)
    df['Total_PnL'] = df['Total_PnL'] - df['Total_PnL'].iloc[0]

    # 2. KPI
    total_costs = df['Transaction_Cost'].sum()
    n_trades = df[df['Action'] != 'HOLD'].shape[0]
    final_pnl = df['Total_PnL'].iloc[-1]
    pnl_volatility = df['Total_PnL'].diff().std()

    print("\n=== PAGELLA STRATEGIA ===")
    print(f"1. Costi Totali:        € {total_costs:.2f}")
    print(f"2. N. Operazioni:       {n_trades}")
    print(f"3. P&L Finale:          € {final_pnl:.2f}")
    print(f"4. Volatilità P&L:      {pnl_volatility:.4f}")

if __name__ == "__main__":
    run_analysis()

# Esempio di utilizzo
# df_whalley = analyze_strategy("whalley_sim_2024-12-20.csv", "Whalley")

# Se avessi un'altra strategia (es. Fixed Time)
# df_fixed = analyze_strategy("fixed_sim_2024-12-20.csv", "Fixed 1h")

# --- CONFRONTO GRAFICO ---
# plt.figure(figsize=(10,6))
# plt.plot(df_whalley['Portfolio_Value'], label='Whalley P&L')
# # plt.plot(df_fixed['Portfolio_Value'], label='Fixed P&L')
# plt.title("Confronto Stabilità Portafoglio (Hedged)")
# plt.legend()
# plt.show()
