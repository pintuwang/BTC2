import os
import json
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone

# 1. Setup Singapore Timezone (UTC+8)
SGT = timezone(timedelta(hours=8))

def calculate_max_pain(ticker_obj, expiry_date):
    try:
        chain = ticker_obj.option_chain(expiry_date)
        total_call_oi = int(chain.calls['openInterest'].sum())
        total_put_oi = int(chain.puts['openInterest'].sum())
        calls = chain.calls[chain.calls['openInterest'] >= 10][['strike', 'openInterest']].fillna(0)
        puts = chain.puts[chain.puts['openInterest'] >= 10][['strike', 'openInterest']].fillna(0)
        strikes = sorted(set(calls['strike']).union(set(puts['strike'])))
        if not strikes: return None, 0, 0
        pain_results = []
        for s in strikes:
            cl = calls[calls['strike'] < s].apply(lambda x: (s - x['strike']) * x['openInterest'], axis=1).sum()
            pl = puts[puts['strike'] > s].apply(lambda x: (x['strike'] - s) * x['openInterest'], axis=1).sum()
            pain_results.append({'strike': s, 'total': cl + pl})
        max_p = float(pd.DataFrame(pain_results).sort_values('total').iloc[0]['strike'])
        return max_p, total_call_oi, total_put_oi
    except: return None, 0, 0

def get_btc_expiry_pains():
    try:
        url = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option"
        resp = requests.get(url, timeout=15).json().get('result', [])
        results = {}
        for item in resp:
            parts = item['instrument_name'].split('-')
            dt = datetime.strptime(parts[1], "%d%b%y").strftime("%Y-%m-%d")
            if dt not in results: results[dt] = float(parts[2])
        return results
    except: return {}

def update_expiry_history(chain_data):
    path = 'data/expiry_history.json'
    history = json.load(open(path)) if os.path.exists(path) else {}
    today = datetime.now(SGT).strftime("%Y-%m-%d") # SGT Date
    
    for entry in chain_data:
        exp = entry['date']
        if exp not in history: history[exp] = []
        if not history[exp] or history[exp][-1]['trade_date'] != today:
            history[exp].append({
                "trade_date": today,
                "mstr_pain": entry['mstr_pain'],
                "btc_pain": entry['btc_pain'],
                "call_oi": entry['call_oi'],
                "put_oi": entry['put_oi']
            })
        history[exp] = history[exp][-10:]

    # Keep 6 months of history (180 days) for historical scrolling
    cutoff = (datetime.now(SGT) - timedelta(days=180)).strftime("%Y-%m-%d")
    history = {k: v for k, v in history.items() if k >= cutoff}
    with open(path, 'w') as f: json.dump(history, f, indent=4)

def run_update():
    mstr = yf.Ticker("MSTR")
    mstr_spot = mstr.history(period="1d")['Close'].iloc[-1]
    btc_dict = get_btc_expiry_pains()
    cutoff = (datetime.now(SGT) + timedelta(days=180)).strftime("%Y-%m-%d")
    
    chain_data = []
    # Process ALL options within the next 180 days
    for exp in [e for e in mstr.options if e <= cutoff]:
        m_pain, m_call_oi, m_put_oi = calculate_max_pain(mstr, exp)
        if m_pain:
            chain_data.append({
                "date": exp, "mstr_pain": round(m_pain, 2), "btc_pain": btc_dict.get(exp, 95000.0),
                "call_oi": m_call_oi, "put_oi": m_put_oi, "is_monthly": (15 <= int(exp.split('-')[2]) <= 21)
            })

    os.makedirs('data', exist_ok=True)
    with open('data/history.json', 'w') as f:
        json.dump({
            "last_update": datetime.now(SGT).strftime("%Y-%m-%d %H:%M"), # SGT Time
            "spot": round(mstr_spot, 2), 
            "data": chain_data
        }, f, indent=4)
    update_expiry_history(chain_data)
    print(f"Sync Complete (SGT). {len(chain_data)} expiries saved.")

if __name__ == "__main__":
    run_update()
