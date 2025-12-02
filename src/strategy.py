import numpy as np
import pandas as pd

class WhalleyHedgingStrategy:
    def __init__(self, risk_aversion: float, transaction_cost: float, initial_cash: float = 0.0):
        self.gamma = risk_aversion
        self.epsilon = transaction_cost
        self.current_shares = 0.0
        self.cash = initial_cash
        self.trade_log = []

    def calculate_bandwidth(self, S, T_rem, r, Gamma_PBS):
        if Gamma_PBS <= 1e-9 or T_rem <= 0: return 0.0
        numerator = 3 * self.epsilon * S * (Gamma_PBS ** 2)
        denominator = 2 * self.gamma
        if numerator < 0: return 0.0
        return (numerator / denominator) ** (1/3)

    # MODIFICATO: Aggiunto parametro Option_Value
    def rebalance(self, timestamp, S, T_rem, r, Delta_PBS, Gamma_PBS, Option_Value):
        H = self.calculate_bandwidth(S, T_rem, r, Gamma_PBS)
        
        target_delta = Delta_PBS
        upper_bound = target_delta + H
        lower_bound = target_delta - H
        
        trade_amount = 0.0
        action = "HOLD"
        
        if self.current_shares > upper_bound:
            desired_shares = upper_bound
            trade_amount = desired_shares - self.current_shares
            action = "SELL"
        elif self.current_shares < lower_bound:
            desired_shares = lower_bound
            trade_amount = desired_shares - self.current_shares
            action = "BUY"
        
        cost = 0.0
        if action != "HOLD":
            transaction_value = abs(trade_amount * S)
            cost = transaction_value * self.epsilon
            self.cash += (-(trade_amount * S)) - cost
            self.current_shares += trade_amount
        
        # LOGGING AGGIORNATO
        self.trade_log.append({
            'timestamp': timestamp,
            'Spot': S,
            'Option_Value': Option_Value,      # <--- SALVIAMO IL PREZZO
            'Ideal_Delta': target_delta,
            'H_Bandwidth': H,
            'Held_Shares': self.current_shares,
            'Action': action,
            'Trade_Size': trade_amount,
            'Transaction_Cost': cost,          # <--- SALVIAMO I COSTI
            'Cash': self.cash
        })

    def get_log_dataframe(self):
        return pd.DataFrame(self.trade_log)