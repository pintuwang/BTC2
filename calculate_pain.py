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
    """Calculates Max Pain strike and total Open Interest with retries."""
    for attempt in range(3):
        try:
            chain = ticker_obj.option_chain(expiry_date)
            total_call_oi = int(chain.calls['openInterest'].sum())
            total_put_oi = int(chain.puts['openInterest'].sum())
            total_oi = total_call_oi + total_put_oi

            # Only retry if OI is ZERO
            if total_oi == 0 and attempt < 2:
                time.sleep(2)
                continue

            calls = chain.calls[chain.calls['openInterest'] >= 10][['strike', 'openInterest']].fillna(0)
            puts = chain.puts[chain.puts['openInterest'] >= 10][['strike', 'openInterest']].fillna(0)
            
            strikes = sorted(set(calls['strike']).union(set(puts['strike'])))
            if not strikes:
                return None, total_call_oi, total_put_oi
            
            pain_results = []
            for s in strikes:
                cl = calls[calls['strike'] < s].apply(lambda x: (s - x['strike']) * x['openInterest'], axis=1).sum()
                pl = puts[puts['strike'] > s].apply(lambda x: (x['strike'] - s) * x['openInterest'], axis=1).sum()
                pain_results.append({'strike': s, 'total': cl + pl})
            
            max_p = float(pd.DataFrame(pain_results).sort_values('total').iloc[0]['strike'])
            return max_p, total_call_oi, total_put_oi
        except:
            if attempt < 2:
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
            if dt not in results:
                results[dt] = float(parts[2])
        return results
    except:
        return {}

def update_expiry_history(chain_data):
    """Maintains a rolling 10-day history for every expiry."""
    path = 'data/expiry_history.json'
    history = json.load(open(path)) if os.path.exists(path) else {}
    today_sgt = datetime.now(SGT).strftime("%Y-%m-%d")
    
    for entry in chain_data:
        exp = entry['date']
        if exp not in history:
            history[exp] = []
        
        # Only add new entry if it's a different day OR if this is the first entry
        if not history[exp] or history[exp][-1]['trade_date'] != today_sgt:
            history[exp].append({
                "trade_date": today_sgt,
                "mstr_pain": entry['mstr_pain'],
                "btc_pain": entry['btc_pain'],
                "call_oi": entry['call_oi'],
                "put_oi": entry['put_oi']
            })
        # Keep last 10 entries
        history[exp] = history[exp][-10:]

    # Clean up expired data older than 180 days
    cutoff = (datetime.now(SGT) - timedelta(days=180)).strftime("%Y-%m-%d")
    history = {k: v for k, v in history.items() if k >= cutoff}
    
    with open(path, 'w') as f:
        json.dump(history, f, indent=4)

def run_update():
    print("=" * 80)
    print(f"MSTR Max Pain Update - {datetime.now(SGT).strftime('%Y-%m-%d %H:%M:%S SGT')}")
    print("=" * 80)
    
    mstr = yf.Ticker("MSTR")
    btc = yf.Ticker("BTC-USD")
    
    try:
        mstr_spot = mstr.history(period="1d")['Close'].iloc[-1]
        btc_spot = btc.history(period="1d")['Close'].iloc[-1]
        print(f"MSTR Spot: ${mstr_spot:.2f} | BTC: ${btc_spot:,.0f}\n")
    except:
        mstr_spot = 165.0
        btc_spot = 75000.0
        print(f"Failed to fetch spots, using defaults\n")

    btc_dict = get_btc_expiry_pains()
    
    # Filter to expiries within next 180 days
    cutoff = (datetime.now(SGT) + timedelta(days=180)).strftime("%Y-%m-%d")
    all_options = [e for e in mstr.options if e <= cutoff]
    
    print(f"Fetching {len(all_options)} expiries...\n")
    
    # SIMPLE APPROACH: Just collect what works right now
    chain_data = []
    for i, exp in enumerate(all_options, 1):
        print(f"[{i}/{len(all_options)}] {exp}...", end=" ")
        m_pain, m_call_oi, m_put_oi = calculate_max_pain(mstr, exp)
        
        if m_pain:
            chain_data.append({
                "date": exp,
                "mstr_pain": round(m_pain, 2),
                "btc_pain": btc_dict.get(exp, 95000.0),
                "call_oi": m_call_oi,
                "put_oi": m_put_oi,
                "is_monthly": (15 <= int(exp.split('-')[2]) <= 21)
            })
            print(f"✓ Pain: ${m_pain:.0f}, OI: {m_call_oi + m_put_oi}")
        else:
            total_oi = m_call_oi + m_put_oi
            if total_oi > 0:
                print(f"⚠️  Has OI ({total_oi}) but no calculable pain")
            else:
                print(f"✗ Zero OI")
        
        time.sleep(0.3)

    os.makedirs('data', exist_ok=True)
    
    # DIRECT SAVE: No reconstruction, just use what we got
    payload = {
        "last_update": datetime.now(SGT).strftime("%Y-%m-%d %H:%M"),
        "spot": round(mstr_spot, 2),
        "btc_spot": round(btc_spot, 2),
        "data": chain_data
    }
    
    with open('data/history.json', 'w') as f:
        json.dump(payload, f, indent=4)

    # Update history AFTER saving payload
    update_expiry_history(chain_data)
    
    # Update spot price log
    log_path = 'data/history_log.json'
    log = json.load(open(log_path)) if os.path.exists(log_path) else []
    today = datetime.now(SGT).strftime("%Y-%m-%d")
    
    if log and log[-1]['date'] == today:
        log[-1].update({"spot": payload["spot"], "btc_spot": payload["btc_spot"]})
    else:
        log.append({"date": today, "spot": payload["spot"], "btc_spot": payload["btc_spot"]})
    
    with open(log_path, 'w') as f:
        json.dump(log[-60:], f, indent=4)
    
    print("\n" + "=" * 80)
    print(f"✓ Update Complete! {len(chain_data)} expiries in chart")
    print("=" * 80)

if __name__ == "__main__":
    run_update()
