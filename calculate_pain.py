import os
import json
import requests
import time
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta, timezone

# --- Singapore Timezone Configuration ---
SGT = timezone(timedelta(hours=8))

def calculate_max_pain(ticker_obj, expiry_date):
    """Calculates Max Pain strike and total Open Interest with Retries."""
    for attempt in range(3):
        try:
            chain = ticker_obj.option_chain(expiry_date)
            total_call_oi = int(chain.calls['openInterest'].sum())
            total_put_oi = int(chain.puts['openInterest'].sum())
            total_oi = total_call_oi + total_put_oi

            # LIQUIDITY GUARD: Rejects obvious glitches where data is missing
            if total_oi < 1000 and attempt < 2:
                time.sleep(2)
                continue

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
        except:
            time.sleep(2)
    return None, 0, 0

def get_btc_expiry_pains():
    """Fetches real BTC Max Pain data from Deribit."""
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
    """Maintains data and prevents overwriting good data with glitches."""
    path = 'data/expiry_history.json'
    history = json.load(open(path)) if os.path.exists(path) else {}
    today_sgt = datetime.now(SGT).strftime("%Y-%m-%d")
    
    for entry in chain_data:
        exp = entry['date']
        if exp not in history: history[exp] = []
        
        # QUALITY CHECK: Don't save if current volume is a tiny fraction of yesterday
        if history[exp]:
            prev = history[exp][-1]
            prev_oi = prev['call_oi'] + prev['put_oi']
            curr_oi = entry['call_oi'] + entry['put_oi']
            if curr_oi < (prev_oi * 0.2) and prev_oi > 5000:
                continue

        if not history[exp] or history[exp][-1]['trade_date'] != today_sgt:
            history[exp].append({
                "trade_date": today_sgt, "mstr_pain": entry['mstr_pain'],
                "btc_pain": entry['btc_pain'], "call_oi": entry['call_oi'], "put_oi": entry['put_oi']
            })
        history[exp] = history[exp][-10:]

    cutoff = (datetime.now(SGT) - timedelta(days=180)).strftime("%Y-%m-%d")
    return {k: v for k, v in history.items() if k >= cutoff}

def run_update():
    mstr = yf.Ticker("MSTR")
    btc = yf.Ticker("BTC-USD")
    
    try:
        mstr_spot = mstr.history(period="1d")['Close'].iloc[-1]
        btc_spot = btc.history(period="1d")['Close'].iloc[-1]
    except:
        mstr_spot, btc_spot = 150.0, 75000.0

    btc_dict = get_btc_expiry_pains()
    all_options = mstr.options
    
    # 1. Fetch live data
    current_chain_data = []
    for exp in all_options:
        m_pain, m_call_oi, m_put_oi = calculate_max_pain(mstr, exp)
        if m_pain:
            current_chain_data.append({
                "date": exp, "mstr_pain": round(m_pain, 2),
                "btc_pain": btc_dict.get(exp, 95000.0),
                "call_oi": m_call_oi, "put_oi": m_put_oi
            })

    # 2. Update permanent archive
    full_history = update_expiry_history(current_chain_data)
    os.makedirs('data', exist_ok=True)
    with open('data/expiry_history.json', 'w') as f:
        json.dump(full_history, f, indent=4)

    # 3. RESTORED PERSISTENCE: Build Chart 1 from Archive to fill gaps
    strategic_list = []
    today_str = datetime.now(SGT).strftime("%Y-%m-%d")
    for exp_date in sorted(full_history.keys()):
        if exp_date < today_str: continue
        latest = full_history[exp_date][-1]
        
        # SANITY CHECK: Only show expiries with logical price ranges
        if latest["mstr_pain"] > (mstr_spot * 0.4) and latest["mstr_pain"] < (mstr_spot * 1.8):
            strategic_list.append({
                "date": exp_date, "mstr_pain": latest["mstr_pain"], "btc_pain": latest["btc_pain"],
                "call_oi": latest["call_oi"], "put_oi": latest["put_oi"],
                "is_monthly": (15 <= int(exp_date.split('-')[2]) <= 21)
            })

    payload = {
        "last_update": datetime.now(SGT).strftime("%Y-%m-%d %H:%M"),
        "spot": round(mstr_spot, 2), "btc_spot": round(btc_spot, 2), "data": strategic_list
    }
    with open('data/history.json', 'w') as f:
        json.dump(payload, f, indent=4)

    # 4. Standard Logging
    log_path = 'data/history_log.json'
    log = json.load(open(log_path)) if os.path.exists(log_path) else []
    today = datetime.now(SGT).strftime("%Y-%m-%d")
    if log and log[-1]['date'] == today:
        log[-1].update({"spot": payload["spot"], "btc_spot": payload["btc_spot"]})
    else:
        log.append({"date": today, "spot": payload["spot"], "btc_spot": payload["btc_spot"]})
    with open(log_path, 'w') as f:
        json.dump(log[-60:], f, indent=4)
    
    print(f"Sync Complete. {len(strategic_list)} valid expiries recorded.")

if __name__ == "__main__":
    run_update()
