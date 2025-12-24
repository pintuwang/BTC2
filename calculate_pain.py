import os
import json
import requests
from datetime import datetime, timedelta

def get_market_phase():
    # 0=Mon, 1=Tue, 2=Wed, 3=Thu, 4=Fri
    day = datetime.now().weekday()
    if day <= 1: return "Provisional (Mon/Tue)"
    if day == 2: return "Sweet Spot (Wednesday)"
    return "Reactive (Thu/Fri)"

def is_third_friday(date_str):
    d = datetime.strptime(date_str, "%Y-%m-%d")
    # Friday is 4, and 3rd Friday must fall between 15th and 21st
    return d.weekday() == 4 and 15 <= d.day <= 21

def run_update():
    # 1. Fetch Real BTC Spot Price
    try:
        btc_res = requests.get("https://www.deribit.com/api/v2/public/get_index_price?index_name=btc_usd").json()
        btc_spot = btc_res['result']['index_price']
    except:
        btc_spot = 95000.0  # Fallback

    # 2. Build 6-Week Forward Chain
    # (Note: In your real environment, replace simulated 'pain' values with your API data)
    base_date = datetime.now()
    days_to_friday = (4 - base_date.weekday() + 7) % 7
    if days_to_friday == 0: days_to_friday = 7
    first_friday = base_date + timedelta(days=days_to_friday)

    chain_data = []
    for i in range(6):
        target_date = (first_friday + timedelta(weeks=i)).strftime("%Y-%m-%d")
        chain_data.append({
            "date": target_date,
            "mstr_pain": 165 + (i * 4), # Non-linear simulation
            "btc_pain": btc_spot + (i * 1100) - (500 if i % 2 == 0 else 0),
            "is_monthly": is_third_friday(target_date)
        })

    payload = {
        "last_update_utc": datetime.utcnow().isoformat(),
        "spot": 178.50, # Replace with live MSTR spot fetch
        "phase": get_market_phase(),
        "data": chain_data
    }

    # Save live data
    os.makedirs('data', exist_ok=True)
    with open('data/history.json', 'w') as f:
        json.dump(payload, f, indent=4)

    # Save to Historical Log (Table)
    log_path = 'data/history_log.json'
    log = json.load(open(log_path)) if os.path.exists(log_path) else []
    new_log = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "spot": payload["spot"],
        "mstr_pain": chain_data[0]["mstr_pain"],
        "phase": payload["phase"]
    }
    if not log or log[-1]['date'] != new_log['date']:
        log.append(new_log)
        with open(log_path, 'w') as f: json.dump(log[-30:], f, indent=4)

if __name__ == "__main__":
    run_update()
