import yfinance as yf
import requests
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timedelta

MSTR_TICKER = "MSTR"
NUM_WEEKS = 8

import requests
import pandas as pd
import numpy as np
import yfinance as yf
import json
from datetime import datetime, timedelta

def get_deribit_btc_max_pain():
    url = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option"
    try:
        response = requests.get(url).json()
        if 'result' not in response: return {}
        data = response['result']
        df = pd.DataFrame(data)
        
        # Normalize Deribit's date format (e.g., '27DEC25') to '2025-12-27'
        def format_date(name):
            try:
                raw_date = name.split('-')[1]
                return datetime.strptime(raw_date, '%d%b%y').strftime('%Y-%m-%d')
            except: return None

        df['date'] = df['instrument_name'].apply(format_date)
        df['strike'] = df['instrument_name'].apply(lambda x: int(x.split('-')[2]))
        df['type'] = df['instrument_name'].apply(lambda x: x.split('-')[3])
        
        results = {}
        for d, group in df.groupby('date'):
            if not d: continue
            strikes = sorted(group['strike'].unique())
            pains = []
            for s in strikes:
                c_loss = group[(group['type'] == 'C') & (group['strike'] < s)].apply(lambda x: (s - x['strike']) * x['open_interest'], axis=1).sum()
                p_loss = group[(group['type'] == 'P') & (group['strike'] > s)].apply(lambda x: (x['strike'] - s) * x['open_interest'], axis=1).sum()
                pains.append(c_loss + p_loss)
            results[d] = float(strikes[np.argmin(pains)])
        return results
    except Exception as e:
        print(f"BTC Data Error: {e}")
        return {}

# [RE-RUN run_monitor() as provided in previous update]

def get_next_fridays(n):
    fridays = []
    d = datetime.now()
    d += timedelta(days=(4 - d.weekday() + 7) % 7)
    for _ in range(n):
        fridays.append(d.strftime('%Y-%m-%d'))
        d += timedelta(days=7)
    return fridays

def calculate_mstr_max_pain(expiry):
    try:
        tk = yf.Ticker(MSTR_TICKER)
        spot = tk.history(period='1d')['Close'].iloc[-1]
        chain = tk.option_chain(expiry)
        
        # FILTER: Only look at strikes within 40% of current price to remove 'junk' data
        calls = chain.calls[(chain.calls['strike'] > spot * 0.6) & (chain.calls['strike'] < spot * 1.4)]
        puts = chain.puts[(chain.puts['strike'] > spot * 0.6) & (chain.puts['strike'] < spot * 1.4)]
        
        strikes = sorted(list(set(calls['strike']).union(set(puts['strike']))))
        pains = []
        for s in strikes:
            c_loss = calls[calls['strike'] < s].apply(lambda x: (s - x['strike']) * x['openInterest'], axis=1).sum()
            p_loss = puts[puts['strike'] > s].apply(lambda x: (x['strike'] - s) * x['openInterest'], axis=1).sum()
            pains.append(c_loss + p_loss)
        return float(strikes[np.argmin(pains)]) if pains else None
    except:
        return None

def get_btc_max_pain_deribit():
    """Fetches all BTC options from Deribit and calculates Max Pain for each expiry."""
    url = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option"
    try:
        data = requests.get(url).json()['result']
        df = pd.DataFrame(data)
        # Extract date from instrument name (e.g., BTC-27DEC24-90000-C)
        df['expiry'] = df['instrument_name'].apply(lambda x: x.split('-')[1])
        df['strike'] = df['instrument_name'].apply(lambda x: int(x.split('-')[2]))
        df['type'] = df['instrument_name'].apply(lambda x: x.split('-')[3])
        
        results = {}
        for expiry_str, group in df.groupby('expiry'):
            try:
                # Convert Deribit date format back to YYYY-MM-DD
                expiry_dt = datetime.strptime(expiry_str, '%d%b%y').strftime('%Y-%m-%d')
                strikes = sorted(group['strike'].unique())
                pains = []
                for s in strikes:
                    # Intrinsic value calculation
                    c_loss = group[(group['type'] == 'C') & (group['strike'] < s)].apply(lambda x: (s - x['strike']) * x['open_interest'], axis=1).sum()
                    p_loss = group[(group['type'] == 'P') & (group['strike'] > s)].apply(lambda x: (x['strike'] - s) * x['open_interest'], axis=1).sum()
                    pains.append(c_loss + p_loss)
                results[expiry_dt] = float(strikes[np.argmin(pains)])
            except: continue
        return results
    except:
        return {}

def run_monitor():
    if not os.path.exists('data'): os.makedirs('data')
    
    fridays = get_next_fridays(NUM_WEEKS)
    mstr_spot = yf.Ticker(MSTR_TICKER).history(period='1d')['Close'].iloc[-1]
    btc_pains_all = get_btc_max_pain_deribit()
    
    final_data = []
    for f in fridays:
        m_pain = calculate_mstr_max_pain(f)
        b_pain = btc_pains_all.get(f) # Match Deribit date to our Friday list
        final_data.append({"date": f, "mstr_pain": m_pain, "btc_pain": b_pain})

    output = {"last_update": datetime.now().strftime("%Y-%m-%d %H:%M"), "spot": float(mstr_spot), "data": final_data}
    with open('data/history.json', 'w') as f:
        json.dump(output, f, indent=4)
    print("Monitor Updated Successfully.")

if __name__ == "__main__":
    run_monitor()
