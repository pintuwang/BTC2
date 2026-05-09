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

def find_nearest_btc_pain(btc_dict, target_date_str, max_days=14):
    """
    Finds the nearest BTC expiry pain within max_days of target_date_str.
    Uses ±14 days to bridge the gap between MSTR 3rd-Friday expiries and
    Deribit's last-Friday monthly expiries (e.g. MSTR Jul-17 → Deribit Jul-31).
    Returns (pain_value, matched_date) or (None, None) if no match found.
    """
    try:
        target = datetime.strptime(target_date_str, "%Y-%m-%d").date()
    except ValueError:
        return None, None

    best_pain = None
    best_date = None
    best_delta = float('inf')

    for bt_date_str, pain in btc_dict.items():
        try:
            bt_date = datetime.strptime(bt_date_str, "%Y-%m-%d").date()
            delta = abs((bt_date - target).days)
            if delta <= max_days and delta < best_delta:
                best_delta = delta
                best_pain = pain
                best_date = bt_date_str
        except ValueError:
            continue

    return best_pain, best_date


def get_btc_expiry_pains():
    """Fetches BTC option OI from Deribit and calculates true Max Pain per expiry."""
    try:
        url = "https://www.deribit.com/api/v2/public/get_book_summary_by_currency?currency=BTC&kind=option"
        resp = requests.get(url, timeout=15).json().get('result', [])
        if not resp:
            print("  ⚠️  Deribit returned empty result")

        # Group OI by expiry → strike → {call_oi, put_oi}
        expiry_data = {}
        for item in resp:
            parts = item['instrument_name'].split('-')
            if len(parts) != 4:
                continue
            dt = datetime.strptime(parts[1], "%d%b%y").strftime("%Y-%m-%d")
            strike = float(parts[2])
            opt_type = parts[3]  # 'C' or 'P'
            oi = item.get('open_interest', 0) or 0

            if dt not in expiry_data:
                expiry_data[dt] = {}
            if strike not in expiry_data[dt]:
                expiry_data[dt][strike] = {'call_oi': 0, 'put_oi': 0}

            if opt_type == 'C':
                expiry_data[dt][strike]['call_oi'] += oi
            elif opt_type == 'P':
                expiry_data[dt][strike]['put_oi'] += oi

        # Calculate max pain for each expiry
        results = {}
        for dt, strikes_dict in expiry_data.items():
            strikes = sorted(strikes_dict.keys())
            if not strikes:
                continue
            best_strike = None
            best_pain = float('inf')
            for s in strikes:
                call_pain = sum(
                    (s - k) * v['call_oi'] for k, v in strikes_dict.items() if k < s
                )
                put_pain = sum(
                    (k - s) * v['put_oi'] for k, v in strikes_dict.items() if k > s
                )
                total = call_pain + put_pain
                if total < best_pain:
                    best_pain = total
                    best_strike = s
            if best_strike is not None:
                results[dt] = best_strike

        print(f"  ✓ Deribit: {len(results)} BTC expiries loaded ({min(results)} → {max(results)})" if results else "  ⚠️  No BTC expiries calculated")
        return results
    except Exception as e:
        print(f"  ✗ Deribit fetch failed: {e}")
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
        # Keep last 10 entries per expiry (evolution tracker snapshots — unchanged)
        history[exp] = history[exp][-10:]

    # ── EXTENDED: retain expiry history for 5 years (was 180 days) ──
    # Enables 1, 2, 3, 4-year rolling analysis of max pain accuracy
    cutoff = (datetime.now(SGT) - timedelta(days=1825)).strftime("%Y-%m-%d")
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
    print(f"BTC dict has {len(btc_dict)} expiry entries\n")
    
    # Filter to expiries within next 180 days
    cutoff = (datetime.now(SGT) + timedelta(days=180)).strftime("%Y-%m-%d")
    all_options = [e for e in mstr.options if e <= cutoff]
    
    print(f"Fetching {len(all_options)} expiries...\n")
    
    # SIMPLE APPROACH: Just collect what works right now
    chain_data = []
    for i, exp in enumerate(all_options, 1):
        print(f"[{i}/{len(all_options)}] {exp}...", end=" ")
        m_pain, m_call_oi, m_put_oi = calculate_max_pain(mstr, exp)
        
        # FIX: Use nearest-date lookup (±14 days) instead of exact match.
        # MSTR 3rd-Friday expiries often don't align with Deribit's last-Friday
        # monthly expiries. A 14-day window bridges gaps like Jul-17 → Jul-31.
        btc_pain_val, btc_matched_date = find_nearest_btc_pain(btc_dict, exp)
        btc_label = f"(matched {btc_matched_date})" if btc_matched_date and btc_matched_date != exp else ""
        
        if m_pain:
            chain_data.append({
                "date": exp,
                "mstr_pain": round(m_pain, 2),
                "btc_pain": btc_pain_val,
                "call_oi": m_call_oi,
                "put_oi": m_put_oi,
                "is_monthly": (15 <= int(exp.split('-')[2]) <= 21)
            })
            btc_str = f"BTC: ${btc_pain_val:,.0f} {btc_label}" if btc_pain_val else "BTC: N/A"
            print(f"✓ MSTR Pain: ${m_pain:.0f}, {btc_str}, OI: {m_call_oi + m_put_oi}")
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
    
    # ── EXTENDED: retain spot log for 5 years (was 60 days) ──
    # Each entry = 1 calendar day. 1825 days = 5 years.
    # Enables Charts 3 & 4 to show full multi-year expiry history
    # and supports future 1/2/3/4-year rolling analysis windows.
    with open(log_path, 'w') as f:
        json.dump(log[-1825:], f, indent=4)
    
    print("\n" + "=" * 80)
    print(f"✓ Update Complete! {len(chain_data)} expiries in chart")
    print("=" * 80)

if __name__ == "__main__":
    run_update()
