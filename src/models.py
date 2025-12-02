import numpy as np
from scipy.stats import norm

def pbs_delta(S, K, T, r, q, sigma):
    """ Calcola il Delta (Practitioner Black-Scholes) """
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    return norm.cdf(d1)

def pbs_gamma(S, K, T, r, q, sigma):
    """ Calcola il Gamma """
    if T <= 0 or sigma <= 0: return 0.0
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    return norm.pdf(d1) / (S * sigma * np.sqrt(T))

def pbs_price(S, K, T, r, q, sigma):
    """ 
    NUOVO: Calcola il Prezzo della Call Europea.
    Necessario per calcolare il valore del portafoglio (P&L).
    """
    if T <= 0 or sigma <= 0: return max(S - K, 0)
    
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    d2 = d1 - sigma * np.sqrt(T)
    
    # Formula Call: S * e^(-qT) * N(d1) - K * e^(-rT) * N(d2)
    price = S * np.exp(-q * T) * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
    return price