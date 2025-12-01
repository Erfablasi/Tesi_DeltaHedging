import numpy as np
from scipy.stats import norm

def pbs_delta(S, K, T, r, q, sigma):
    """
    Calcola il Delta di una Call Europea (Practitioner Black-Scholes).
    Se usi il Forward Price nel main, passa q=0.
    """
    if T <= 0 or sigma <= 0:
        return 0.0
    
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    return norm.cdf(d1)

def pbs_gamma(S, K, T, r, q, sigma):
    """
    [cite_start]Calcola il Gamma. Formula standard[cite: 51].
    """
    if T <= 0 or sigma <= 0:
        return 0.0

    d1 = (np.log(S / K) + (r - q + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
    return norm.pdf(d1) / (S * sigma * np.sqrt(T))