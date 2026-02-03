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
    print(f"\n=== Fetching {expiry_date} ===")
    
    for attempt in range(3):
        try:
            chain = ticker_obj.option_chain(expiry_date)
            
            # Debug: Show raw data
            print(f"  Attempt {attempt + 1}: Calls shape={chain.calls.shape}, Puts shape={chain.puts.shape}")
            
            total_call_oi = int(chain.calls['openInterest'].sum())
            total_put_oi = int(chain.puts['openInterest'].sum())
            total_oi = total_call_oi + total_put_oi
            
            print(f"  Total OI: {total_oi} (Calls: {total_call_oi}, Puts: {total_put_oi})")

            # Only retry if OI is ZERO (clear fetch failure)
            if total_oi == 0 and attempt < 2:
                print(f"  Zero OI detected, retrying...")
                time.sleep(2)
                continue

            calls = chain.calls[chain.calls['openInterest'] >= 10][['strike', 'openInterest']].fillna(0)
            puts = chain.puts[chain.puts['openInterest'] >= 10][['strike', 'openInterest']].fillna(0)
            
            print(f"  Strikes with OI >= 10: Calls={len(calls)}, Puts={len(puts)}")
            
            strikes = sorted(set(calls['strike']).union(set(puts['strike'])))
            if not strikes: 
                print(f"  WARNING: No strikes meet OI >= 10 threshold")
                return None, total_call_oi, total_put_oi
            
            pain_results = []
            for s in strikes:
                cl = calls[calls['strike'] < s].apply(lambda x: (s - x['strike']) * x['openInterest'], axis=1).sum()
                pl = puts[puts['strike'] > s].apply(lambda x: (x['strike'] - s) * x['openInterest'], axis=1).sum()
                pain_results.append({'strike': s, 'total': cl + pl})
            
            max_p = float(pd.DataFrame(pain_results).sort_values('total').iloc[0]['strike'])
            print(f"  ✓ Max Pain: {max_p}")
            return max_p, total_call_oi, total_put_oi
            
        except Exception as e:
            print(f"  ERROR on attempt {attempt + 1}: {e}")
            if attempt == 2:
                print(f"  FAILED after 3 attempts")
            time.sleep(2)
    
    return None, 0, 0

def run_diagnostic():
    print("=" * 80)
    print("DIAGNOSTIC RUN - Checking all MSTR option expiries")
    print("=" * 80)
    
    mstr = yf.Ticker("MSTR")
    
    try:
        mstr_spot = mstr.history(period="1d")['Close'].iloc[-1]
        print(f"\nMSTR Spot Price: ${mstr_spot:.2f}\n")
    except:
        mstr_spot = 150.0
        print(f"\nFailed to fetch spot, using default: ${mstr_spot:.2f}\n")

    all_options = mstr.options
    print(f"Total expiries available from yfinance: {len(all_options)}")
    print(f"Expiries: {all_options}\n")
    
    results_summary = []
    
    for exp in all_options:
        m_pain, m_call_oi, m_put_oi = calculate_max_pain(mstr, exp)
        
        results_summary.append({
            'expiry': exp,
            'max_pain': m_pain,
            'call_oi': m_call_oi,
            'put_oi': m_put_oi,
            'total_oi': m_call_oi + m_put_oi,
            'status': 'OK' if m_pain else ('NO_STRIKES' if (m_call_oi + m_put_oi) > 0 else 'FAILED')
        })
        
        # Small delay between fetches to avoid rate limiting
        time.sleep(0.5)
    
    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    df = pd.DataFrame(results_summary)
    print(df.to_string(index=False))
    
    print(f"\nStatistics:")
    print(f"  Total expiries: {len(results_summary)}")
    print(f"  Successful (with max pain): {len(df[df['status'] == 'OK'])}")
    print(f"  Has OI but no strikes: {len(df[df['status'] == 'NO_STRIKES'])}")
    print(f"  Completely failed: {len(df[df['status'] == 'FAILED'])}")
    
    # Save diagnostic results
    os.makedirs('data', exist_ok=True)
    with open('data/diagnostic_results.json', 'w') as f:
        json.dump(results_summary, f, indent=4)
    
    print(f"\nDiagnostic results saved to data/diagnostic_results.json")

if __name__ == "__main__":
    run_diagnostic()
