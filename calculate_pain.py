import os
import json
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def calculate_max_pain(ticker_obj, expiry_date):
    """Calculates Max Pain strike and total Open Interest."""
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
            call_loss = calls[calls['strike'] < s].apply(lambda x: (s - x['strike']) * x['openInterest'], axis=1).sum()
            put_loss = puts[puts['strike'] > s].apply(lambda x: (x['strike'] - s) * x['openInterest'], axis=1).sum()
            pain_results.append({'strike': s, 'total': call_loss + put_loss})
        
        max_pain_strike = float(pd.DataFrame(pain_results).sort_values('total').iloc[0]['strike'])
        
        return max_pain_strike, total_call_oi, total_put_oi
    except Exception as e:
        print(f"Error calculating MSTR pain for {expiry_date}: {e}")
        return None, 0, 0

def get_btc_expiry_pains():
    """Fetches real BTC Max Pain data from Deribit."""
    try:
        url = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option"
        resp = requests.get(url, timeout=15).json()
        data = resp.get('result', [])
        
        exp_groups = {}
        for item in data:
            parts = item['instrument_name'].split('-')
            exp_str = parts[1]
            strike = float(parts[2])
            oi = item['open_interest']
            side = parts[3] 
            
            if exp_str not in exp_groups: 
                exp_groups[exp_str] = {'calls': [], 'puts': [], 'strikes': set()}
            
            exp_groups[exp_str]['strikes'].add(strike)
            if side == 'C':
                exp_groups[exp_str]['calls'].append({'strike': strike, 'oi': oi})
            else:
                exp_groups[exp_str]['puts'].append({'strike': strike, 'oi': oi})

        results = {}
        for exp, val in exp_groups.items():
            try:
                dt = datetime.strptime(exp, "%d%b%y").strftime("%Y-%m-%d")
                strikes = sorted(list(val['strikes']))
                pains = []
                for s in strikes:
                    cl = sum((s - c['strike']) * c['oi'] for c in val['calls'] if c['strike'] < s)
                    pl = sum((p['strike'] - s) * p['oi'] for p in val['puts'] if p['strike'] > s)
                    pains.append({'strike': s, 'total': cl + pl})
                results[dt] = sorted(pains, key=lambda x: x['total'])[0]['strike']
            except: continue
        return results
    except Exception as e:
        print(f"BTC Data Fetch Error: {e}")
        return {}

def update_expiry_history(chain_data):
    """Saves the last 10 trading days of data for each specific expiry."""
    path = 'data/expiry_history.json'
    history = json.load(open(path)) if os.path.exists(path) else {}
    today = datetime.now().strftime("%Y-%m-%d")

    for entry in chain_data:
        exp = entry['date']
        if exp not in history: history[exp] = []
        
        # Avoid duplicate entries for the same day
        if not history[exp] or history[exp][-1]['trade_date'] != today:
            history[exp].append({
                "trade_date": today,
                "mstr_pain": entry['mstr_pain'],
                "btc_pain": entry['btc_pain'],
                "call_oi": entry['call_oi'],
                "put_oi": entry['put_oi']
            })
        
        # Maintain only 10 most recent days per expiry
        history[exp] = history[exp][-10:]

    # Clean up passed expiries
    history = {k: v for k, v in history.items() if k >= today}
    
    with open(path, 'w') as f:
        json.dump(history, f, indent=4)

def run_update():
    mstr = yf.Ticker("MSTR")
    try:
        mstr_spot = mstr.history(period="1d")['Close'].iloc[-1]
    except:
        mstr_spot = 165.0

    btc_dict = get_btc_expiry_pains()
    sorted_btc_dates = sorted(btc_dict.keys())

    # Expanded to 180 days (6 months) for the new trend tracker
    cutoff = (datetime.now() + timedelta(days=180)).strftime("%Y-%m-%d")
    all_options = [e for e in mstr.options if e <= cutoff]
    
    chain_data = []
    for exp in all_options:
        m_pain, m_call_oi, m_put_oi = calculate_max_pain(mstr, exp)
        if m_pain:
            b_pain = btc_dict.get(exp)
            if not b_pain and sorted_btc_dates:
                closest_date = min(sorted_btc_dates, key=lambda d: abs(datetime.strptime(d, "%Y-%m-%d") - datetime.strptime(exp, "%Y-%m-%d")))
                b_pain = btc_dict[closest_date]
            
            chain_data.append({
                "date": exp,
                "mstr_pain": round(m_pain, 2),
                "btc_pain": round(b_pain or 95000.0, 2),
                "call_oi": m_call_oi,
                "put_oi": m_put_oi,
                "is_monthly": (15 <= int(exp.split('-')[2]) <= 21)
            })

    # Save Current Snapshot
    payload = {
        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "last_update_utc": datetime.utcnow().isoformat() + "Z",
        "spot": round(mstr_spot, 2),
        "data": chain_data
    }

    os.makedirs('data', exist_ok=True)
    with open('data/history.json', 'w') as f:
        json.dump(payload, f, indent=4)

    # Update 10-Day Trend Tracker
    update_expiry_history(chain_data)

    # Update Daily History Log
    log_path = 'data/history_log.json'
    log = json.load(open(log_path)) if os.path.exists(log_path) else []
    today = datetime.now().strftime("%Y-%m-%d")
    if not log or log[-1]['date'] != today:
        log.append({"date": today, "spot": payload["spot"]})
        with open(log_path, 'w') as f:
            json.dump(log[-60:], f, indent=4)
    
    print(f"Update Finished. {len(chain_data)} expiries tracked.")

if __name__ == "__main__":
    run_update()
