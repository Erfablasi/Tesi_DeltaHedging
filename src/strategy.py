import numpy as np
import pandas as pd

class WhalleyHedgingStrategy:
    def __init__(self, risk_aversion: float, transaction_cost: float, initial_cash: float = 0.0):
        """
        [cite_start]Implementa la strategia asintotica di Whalley & Wilmott[cite: 3].
        risk_aversion (gamma): es. 1.0
        transaction_cost (epsilon): es. 0.002
        """
        self.gamma = risk_aversion
        self.epsilon = transaction_cost
        
        # Stato del portafoglio
        self.current_shares = 0.0
        self.cash = initial_cash
        self.trade_log = []

    def calculate_bandwidth(self, S, T_rem, r, Gamma_PBS):
        """
        [cite_start]Calcola la semi-ampiezza della banda H[cite: 240, 325].
        Formula: H = ( (3 * eps * S * Gamma^2) / (2 * gamma) )^(1/3)
        Nota: Abbiamo semplificato il fattore di sconto per T_rem piccoli.
        """
        if Gamma_PBS <= 1e-9 or T_rem <= 0:
            return 0.0
        
        # Formula 3.10 del paper
        numerator = 3 * self.epsilon * S * (Gamma_PBS ** 2)
        denominator = 2 * self.gamma
        
        # Protezione matematica per evitare radici di numeri negativi (anche se improbabile qui)
        if numerator < 0: return 0.0
        
        bandwidth = (numerator / denominator) ** (1/3)
        return bandwidth

    def rebalance(self, timestamp, S, T_rem, r, Delta_PBS, Gamma_PBS):
        # 1. Calcola la Banda Ottimale
        H = self.calculate_bandwidth(S, T_rem, r, Gamma_PBS)
        
        # 2. Definisci i confini attorno al Delta Teorico (y*)
        target_delta = Delta_PBS
        upper_bound = target_delta + H
        lower_bound = target_delta - H
        
        trade_amount = 0.0
        action = "HOLD"
        
        # 3. Logica "Reflecting Barrier" (Vendi solo se tocchi il tetto, Compra solo se tocchi il fondo)
        if self.current_shares > upper_bound:
            desired_shares = upper_bound
            trade_amount = desired_shares - self.current_shares # Negativo = Vendita
            action = "SELL"
            
        elif self.current_shares < lower_bound:
            desired_shares = lower_bound
            trade_amount = desired_shares - self.current_shares # Positivo = Acquisto
            action = "BUY"
        
        # 4. Esecuzione (Simulata)
        if action != "HOLD":
            transaction_value = abs(trade_amount * S)
            cost = transaction_value * self.epsilon
            # Aggiorna Cash: (segno opposto al trade) - costi
            self.cash += (-(trade_amount * S)) - cost
            self.current_shares += trade_amount
        
        # 5. Salva Log
        self.trade_log.append({
            'timestamp': timestamp,
            'Spot': S,
            'Ideal_Delta': target_delta,
            'H_Bandwidth': H,
            'Held_Shares': self.current_shares,
            'Action': action,
            'Trade_Size': trade_amount,
            'Cash': self.cash
        })

    def get_log_dataframe(self):
        return pd.DataFrame(self.trade_log)