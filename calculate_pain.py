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

            # Only retry if OI is ZERO (clear fetch failure)
            if total_oi == 0 and attempt < 2:
                time.sleep(2)
                continue

            calls = chain.calls[chain.calls['openInterest'] >= 10][['strike', 'openInterest']].fillna(0)
            puts = chain.puts[chain.puts['openInterest'] >= 10][['strike', 'openInterest']].fillna(0)
            
            strikes = sorted(set(calls['strike']).union(set(puts['strike'])))
            if not strikes: 
                # Return OI even if max pain can't be calculated
                return None, total_call_oi, total_put_oi
            
            pain_results = []
            for s in strikes:
                cl = calls[calls['strike'] < s].apply(lambda x: (s - x['strike']) * x['openInterest'], axis=1).sum()
                pl = puts[puts['strike'] > s].apply(lambda x: (x['strike'] - s) * x['openInterest'], axis=1).sum()
                pain_results.append({'strike': s, 'total': cl + pl})
            
            max_p = float(pd.DataFrame(pain_results).sort_values('total').iloc[0]['strike'])
            return max_p, total_call_oi, total_put_oi
        except Exception as e:
            if attempt == 2:
                print(f"Failed to fetch {expiry_date}: {e}")
            time.sleep(2)
    return None, 0, 0

def update_expiry_history(chain_data):
    """Maintains data and prevents overwriting good data with glitches."""
    path = 'data/expiry_history.json'
    history = json.load(open(path)) if os.path.exists(path) else {}
    today_sgt = datetime.now(SGT).strftime("%Y-%m-%d")
    
    for entry in chain_data:
        exp = entry['date']
        if exp not in history: history[exp] = []
        
        # More lenient quality check - only block obvious zeros
        should_skip = False
        if history[exp]:
            prev = history[exp][-1]
            prev_oi = prev['call_oi'] + prev['put_oi']
            curr_oi = entry['call_oi'] + entry['put_oi']
            
            # Only skip if current OI is zero but previous was substantial
            if curr_oi == 0 and prev_oi > 1000:
                should_skip = True
                print(f"Skipping {exp}: Zero OI (prev: {prev_oi})")
        
        if should_skip:
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

    all_options = mstr.options
    current_chain_data = []
    
    # CRITICAL FIX: Save ALL expiries with any OI data, not just those with calculable max pain
    for exp in all_options:
        m_pain, m_call_oi, m_put_oi = calculate_max_pain(mstr, exp)
        
        # Changed condition: Save if there's ANY open interest
        if m_call_oi > 0 or m_put_oi > 0:
            # Use previous max pain if current calculation failed
            current_chain_data.append({
                "date": exp, 
                "mstr_pain": round(m_pain, 2) if m_pain else None,
                "btc_pain": 95000.0, 
                "call_oi": m_call_oi, 
                "put_oi": m_put_oi
            })
            if m_pain is None:
                print(f"Warning: {exp} has OI ({m_call_oi + m_put_oi}) but no calculable max pain")

    full_history = update_expiry_history(current_chain_data)
    os.makedirs('data', exist_ok=True)
    with open('data/expiry_history.json', 'w') as f:
        json.dump(full_history, f, indent=4)

    # RECONSTRUCT CHART 1: Fill gaps using Persistence from History
    strategic_list = []
    today_str = datetime.now(SGT).strftime("%Y-%m-%d")
    
    for exp_date in sorted(full_history.keys()):
        if exp_date < today_str: 
            continue
        
        latest = full_history[exp_date][-1]
        
        # Use historical max pain if current is None
        mstr_pain_value = latest["mstr_pain"]
        
        # If no max pain available at all, estimate from spot
        if mstr_pain_value is None:
            # Look back through history for last valid max pain
            for historical_entry in reversed(full_history[exp_date]):
                if historical_entry["mstr_pain"] is not None:
                    mstr_pain_value = historical_entry["mstr_pain"]
                    print(f"Using historical max pain for {exp_date}: {mstr_pain_value}")
                    break
            
            # If still no max pain found, use spot as fallback
            if mstr_pain_value is None:
                mstr_pain_value = round(mstr_spot, 2)
                print(f"Using spot price as fallback for {exp_date}: {mstr_pain_value}")
        
        # SANITY CHECK: Only show expiries with logical price ranges
        if mstr_pain_value > (mstr_spot * 0.4) and mstr_pain_value < (mstr_spot * 1.8):
            strategic_list.append({
                "date": exp_date, 
                "mstr_pain": mstr_pain_value, 
                "btc_pain": latest["btc_pain"],
                "call_oi": latest["call_oi"], 
                "put_oi": latest["put_oi"],
                "is_monthly": (15 <= int(exp_date.split('-')[2]) <= 21)
            })

    payload = {
        "last_update": datetime.now(SGT).strftime("%Y-%m-%d %H:%M"),
        "spot": round(mstr_spot, 2), 
        "btc_spot": round(btc_spot, 2), 
        "data": strategic_list
    }
    with open('data/history.json', 'w') as f:
        json.dump(payload, f, indent=4)

    log_path = 'data/history_log.json'
    log = json.load(open(log_path)) if os.path.exists(log_path) else []
    today = datetime.now(SGT).strftime("%Y-%m-%d")
    if log and log[-1]['date'] == today:
        log[-1].update({"spot": payload["spot"], "btc_spot": payload["btc_spot"]})
    else:
        log.append({"date": today, "spot": payload["spot"], "btc_spot": payload["btc_spot"]})
    with open(log_path, 'w') as f:
        json.dump(log[-60:], f, indent=4)
    
    print(f"Update Finished. {len(strategic_list)} valid expiries recorded from {len(all_options)} total.")

if __name__ == "__main__":
    run_update()
