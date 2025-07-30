import json
from pprint import pprint
from math import erf, exp, log, sqrt


def black_schole_merton(S=0, K=0, r=0, T=0, sigma=0) -> float:
    '''
    S[float]: current stock price
    K[float]: option strike price
    r[float]: risk-free interest rate
    T[int]: time remaining until expiration
    sigma[float]: underlying security’s volatility
    '''
    def norm_cdf(x):
        return 0.5 * (1 + erf(x / sqrt(2)))
    
    if K == 0 or sigma == 0 or T == 0:
        return None
    
    S = float(S)
    K = float(K)
    T = float(T)
    sigma = float(sigma)

    d1 = (log(S/K) + (r + sigma**2 * 0.5) * T) / (sigma * sqrt(T))
    d2 = d1 - sigma * sqrt(T)
    N_d1 = norm_cdf(d1)
    N_d2 = norm_cdf(d2)
    call_option_price = N_d1 * S - N_d2 * K * exp(-r * T)
    return call_option_price


def lambda_handler(event, context):
    print("Received event: " + json.dumps(event, indent=2))
    print("current stock price = ", event['S'])
    print("the option strike price = ", event['K'])
    print("risk-free interest rate = ", event['r'])
    print("time remaining until expiration = ", event['T'])
    print("underlying security’s volatility = ", event['sigma'])
    return black_schole_merton(S=event['S'], K=event['K'], r=event['r'], T=event['T'], sigma=event['sigma'])
    raise Exception('Something went wrong')
