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

def update_expiry_history(chain_data, existing_history):
    """Maintains data and prevents overwriting good data with bad fetches."""
    today_sgt = datetime.now(SGT).strftime("%Y-%m-%d")
    
    for entry in chain_data:
        exp = entry['date']
        curr_oi = entry['call_oi'] + entry['put_oi']
        
        if exp not in existing_history:
            existing_history[exp] = []
        
        # CRITICAL: Don't overwrite good data with zero OI from bad fetches
        should_skip = False
        
        if existing_history[exp]:
            prev = existing_history[exp][-1]
            prev_oi = prev['call_oi'] + prev['put_oi']
            
            # Skip if yfinance returned zero but we had good data before
            if curr_oi == 0 and prev_oi > 50:
                print(f"⚠️  Skipping {exp}: yfinance returned 0 OI (previous: {prev_oi})")
                should_skip = True
            
            # Skip if OI dropped by >80% (likely a data glitch)
            elif curr_oi > 0 and curr_oi < (prev_oi * 0.2) and prev_oi > 1000:
                print(f"⚠️  Skipping {exp}: Suspicious OI drop {prev_oi} → {curr_oi}")
                should_skip = True
        
        if should_skip:
            continue

        # Update or append new data point
        if not existing_history[exp] or existing_history[exp][-1]['trade_date'] != today_sgt:
            existing_history[exp].append({
                "trade_date": today_sgt,
                "mstr_pain": entry['mstr_pain'],
                "btc_pain": entry['btc_pain'],
                "call_oi": entry['call_oi'],
                "put_oi": entry['put_oi']
            })
            print(f"✓ Updated {exp}: Max Pain ${entry['mstr_pain']}, OI: {curr_oi}")
        
        # Keep last 10 data points
        existing_history[exp] = existing_history[exp][-10:]

    # Clean up old expiries
    cutoff = (datetime.now(SGT) - timedelta(days=180)).strftime("%Y-%m-%d")
    return {k: v for k, v in existing_history.items() if k >= cutoff}

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
        mstr_spot, btc_spot = 150.0, 75000.0
        print(f"Failed to fetch spots, using defaults\n")

    # Load existing history first
    history_path = 'data/expiry_history.json'
    existing_history = json.load(open(history_path)) if os.path.exists(history_path) else {}
    
    all_options = mstr.options
    print(f"Fetching {len(all_options)} expiries from yfinance...\n")
    
    # Fetch current data
    current_chain_data = []
    for i, exp in enumerate(all_options, 1):
        print(f"[{i}/{len(all_options)}] {exp}...", end=" ")
        m_pain, m_call_oi, m_put_oi = calculate_max_pain(mstr, exp)
        
        total_oi = m_call_oi + m_put_oi
        
        if m_pain:
            current_chain_data.append({
                "date": exp,
                "mstr_pain": round(m_pain, 2),
                "btc_pain": 95000.0,
                "call_oi": m_call_oi,
                "put_oi": m_put_oi
            })
            print(f"✓ Pain: ${m_pain:.0f}, OI: {total_oi}")
        elif total_oi > 0:
            # Has OI but couldn't calculate pain (too few strikes)
            current_chain_data.append({
                "date": exp,
                "mstr_pain": None,
                "btc_pain": 95000.0,
                "call_oi": m_call_oi,
                "put_oi": m_put_oi
            })
            print(f"⚠️  Has OI ({total_oi}) but no calculable pain")
        else:
            print(f"✗ Zero OI returned")
        
        time.sleep(0.3)  # Rate limit protection
    
    print("\n" + "=" * 80)
    print("Updating history...")
    print("=" * 80 + "\n")
    
    # Update history with new data
    full_history = update_expiry_history(current_chain_data, existing_history)
    
    # Save updated history
    os.makedirs('data', exist_ok=True)
    with open('data/expiry_history.json', 'w') as f:
        json.dump(full_history, f, indent=4)

    # Build final payload from history (with gap filling)
    print("\n" + "=" * 80)
    print("Building chart payload...")
    print("=" * 80 + "\n")
    
    strategic_list = []
    today_str = datetime.now(SGT).strftime("%Y-%m-%d")
    
    for exp_date in sorted(full_history.keys()):
        if exp_date < today_str:
            continue
        
        latest = full_history[exp_date][-1]
        mstr_pain_value = latest["mstr_pain"]
        
        # If no max pain, try to find historical value
        if mstr_pain_value is None:
            for historical_entry in reversed(full_history[exp_date]):
                if historical_entry["mstr_pain"] is not None:
                    mstr_pain_value = historical_entry["mstr_pain"]
                    print(f"Using historical pain for {exp_date}: ${mstr_pain_value}")
                    break
            
            # Last resort: use spot
            if mstr_pain_value is None:
                mstr_pain_value = round(mstr_spot, 2)
                print(f"Using spot as fallback for {exp_date}: ${mstr_pain_value}")
        
        # Sanity check on price range
        if mstr_pain_value > (mstr_spot * 0.4) and mstr_pain_value < (mstr_spot * 1.8):
            strategic_list.append({
                "date": exp_date,
                "mstr_pain": mstr_pain_value,
                "btc_pain": latest["btc_pain"],
                "call_oi": latest["call_oi"],
                "put_oi": latest["put_oi"],
                "is_monthly": (15 <= int(exp_date.split('-')[2]) <= 21)
            })

    # Save final payload
    payload = {
        "last_update": datetime.now(SGT).strftime("%Y-%m-%d %H:%M"),
        "spot": round(mstr_spot, 2),
        "btc_spot": round(btc_spot, 2),
        "data": strategic_list
    }
    with open('data/history.json', 'w') as f:
        json.dump(payload, f, indent=4)

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
    print(f"✓ Update Complete!")
    print(f"  - {len(strategic_list)} expiries in final chart")
    print(f"  - {len(full_history)} expiries tracked in history")
    print(f"  - Payload saved to data/history.json")
    print("=" * 80)

if __name__ == "__main__":
    run_update()
