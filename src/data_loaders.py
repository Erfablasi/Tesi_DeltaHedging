import pandas as pd
import numpy as np

class VolatilityManager:
    def __init__(self, filepath):
        print(f"Loading Volatility Surface: {filepath}")
        
        # 1. LETTURA PARQUET
        self.df = pd.read_parquet(filepath)
        
        # 2. NORMALIZZAZIONE NOMI COLONNE
        rename_map = {
            'Expiry': 'expiry_date',
            'Strike': 'strike',
            'IV': 'iv',
            'Moneyness': 'moneyness'
        }
        self.df = self.df.rename(columns=rename_map)
        
        # 3. CONVERSIONE DATE
        self.df['AsOfDate'] = pd.to_datetime(self.df['AsOfDate'])
        self.df['expiry_date'] = pd.to_datetime(self.df['expiry_date'])
        
        # 4. INDICIZZAZIONE
        self.df = self.df.sort_values(by=['AsOfDate', 'expiry_date', 'strike'])
        self.df.set_index(['AsOfDate', 'expiry_date'], inplace=True)
        self.df = self.df.sort_index()
        
        print(f"   Volatilità caricata. Date: da {self.df.index.levels[0].min().date()} a {self.df.index.levels[0].max().date()}")

    def get_interpolated_iv(self, current_date, expiry_date, target_strike):
        """
        Cerca la IV corretta sulla superficie statica del giorno.
        Usa argmin sui valori per evitare ambiguità di indici duplicati.
        """
        try:
            # 1. Seleziona la 'fetta' di oggi per quella scadenza
            subset = self.df.loc[(current_date, expiry_date)]
            
            # 2. Trova la POSIZIONE (0, 1, 2...) dello strike più vicino
            # .values converte in array NumPy, .argmin() ci dà l'intero posizionale
            pos_nearest = (subset['strike'] - target_strike).abs().values.argmin()
            
            # 3. Restituisci la IV usando la posizione (.iloc)
            return subset.iloc[pos_nearest]['iv']
            
        except KeyError:
            return None
        except Exception:
            return None

class RatesManager:
    def __init__(self, filepath):
        print(f"Loading Rates: {filepath}")
        self.df = pd.read_parquet(filepath)
        self.df['AsOfDate'] = pd.to_datetime(self.df['AsOfDate'])
        self.df = self.df.set_index('AsOfDate').sort_index()
    
    def get_risk_free_rate(self, date, tenor_days):
        try:
            curve = self.df.loc[date]
            if not isinstance(curve, pd.Series): 
                return np.interp(tenor_days, curve['tau_days'], curve['r'])
            else:
                return curve['r']
        except:
            return 0.03

class DividendsManager:
    def __init__(self, filepath):
        print(f"Loading Dividends: {filepath}")
        self.df = pd.read_parquet(filepath)
        self.df['AsOfDate'] = pd.to_datetime(self.df['AsOfDate'])
        self.df = self.df.set_index('AsOfDate').sort_index()
        
        if 'q' in self.df.columns:
            self.df['q'] = self.df['q'].replace(0.0, np.nan).ffill()

    def get_yield_q(self, date):
        try:
            idx = self.df.index.get_indexer([date], method='ffill')[0]
            return self.df.iloc[idx]['q'] if idx != -1 else 0.03
        except:
            return 0.03