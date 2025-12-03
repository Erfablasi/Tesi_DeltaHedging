import numpy as np
import pandas as pd

class AdaptiveLossStrategy:
    def __init__(self, risk_aversion_weight: float = 0.5, transaction_cost: float = 0.002):
        """
        Strategia Proprietaria 'Adaptive Loss Minimizer'.
        Invece di bande fisse (Whalley), minimizza una Loss Function istantanea.
        
        risk_aversion_weight (lambda): 
            0.5 = Bilanciato (Standard)
            > 0.5 = Odia i costi (Fa meno hedging)
            < 0.5 = Odia il rischio (Fa più hedging)
        """
        self.lam = risk_aversion_weight
        self.epsilon = transaction_cost
        
        self.current_shares = 0.0
        self.cash = 0.0
        self.trade_log = []

    def rebalance(self, timestamp, S, T_rem, r, Delta_PBS, Gamma_PBS, Volatility_IV, dt_hours, Option_Value):
        """
        Calcola se conviene agire ORA confrontando:
        Loss_Wait (Rischio Varianza futuro) vs Loss_Trade (Costo certo immediato).
        """
        
        # 1. Calcolo Sbilanciamento (Gap)
        delta_gap = self.current_shares - Delta_PBS
        
        # 2. STIMA DEL RISCHIO (Loss se aspetto)
        # Teoria: La varianza del P&L di un portafoglio delta-hedged non perfettamente
        # è approssimabile come: Var ~ (0.5 * Gamma * dS^2) ... 
        # Ma per un modello Gradient semplice, usiamo la varianza del Delta Gap.
        # Rischio € ~ (Delta_Gap * S * Vol * sqrt(dt))
        # Loss_Wait = (1 - lambda) * Varianza Attesa
        
        # Convertiamo dt in anni
        dt_years = dt_hours / (24 * 365.25)
        if dt_years <= 0: dt_years = 1.0 / (24*365.25*60) # fallback 1 min
        
        # Varianza attesa in Euro^2
        expected_variance = (delta_gap * S * Volatility_IV) ** 2 * dt_years
        
        # Score di Rischio (pesato)
        loss_wait = (1 - self.lam) * expected_variance
        
        # 3. STIMA DEL COSTO (Loss se agisco)
        # Costo Certo = |Gap| * S * epsilon
        trade_cost_cash = abs(delta_gap * S) * self.epsilon
        
        # Score di Costo (pesato)
        loss_trade = self.lam * trade_cost_cash
        
        # 4. DECISIONE (Minimizzazione Loss)
        # Se il rischio di aspettare "costa" più della commissione -> TRADE
        action = "HOLD"
        trade_amount = 0.0
        actual_cost = 0.0
        
        # Trigger Condition (Il Gradiente punta verso il Trade)
        if loss_wait > loss_trade:
            # Chiudiamo il gap
            trade_amount = -delta_gap # Se gap positivo (ho troppo), vendo
            
            # Filtro micro-trade (rumore numerico)
            if abs(trade_amount) > 0.0001:
                if trade_amount > 0: action = "BUY"
                else: action = "SELL"
                
                actual_cost = abs(trade_amount * S) * self.epsilon
                self.cash += (-(trade_amount * S)) - actual_cost
                self.current_shares += trade_amount
        
        # 5. LOGGING (Cruciale per il confronto)
        self.trade_log.append({
            'timestamp': timestamp,
            'Spot': S,
            'Option_Value': Option_Value,
            'Ideal_Delta': Delta_PBS,
            'Held_Shares': self.current_shares,
            'Action': action,
            'Trade_Size': trade_amount,
            'Transaction_Cost': actual_cost,
            'Loss_Wait_Score': loss_wait,   # Utile per debugging
            'Loss_Trade_Score': loss_trade, # Utile per debugging
            'Cash': self.cash
        })

    def get_log_dataframe(self):
        return pd.DataFrame(self.trade_log)