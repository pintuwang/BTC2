import os
import json
import requests
from datetime import datetime

def get_max_pain(symbol):
    # This is a simplified proxy. In your actual script, 
    # ensure you are looping through the next 6 expiries.
    # Logic: Fetch option chain -> Calculate sum of losses per strike -> find minimum.
    expiries = ["2025-12-26", "2026-01-02", "2026-01-09", "2026-01-16", "2026-01-23", "2026-01-30"]
    data = []
    for date in expiries:
        # Placeholder for actual API logic
        data.append({
            "date": date,
            "mstr_pain": 160 + (len(data) * 5), # Simulated trend
            "btc_pain": 95000 + (len(data) * 1000)
        })
    return data

def update_files():
    # 1. GET CURRENT MARKET DATA
    # Replace with your actual API calls
    spot_price = 175.50 
    chain_data = get_max_pain("MSTR")

    full_payload = {
        "last_update": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "spot": spot_price,
        "data": chain_data # This restores the 6-week X-Axis
    }

    # 2. SAVE FORWARD-LOOKING CHART DATA
    os.makedirs('data', exist_ok=True)
    with open('data/history.json', 'w') as f:
        json.dump(full_payload, f, indent=4)

    # 3. SAVE HISTORICAL SNAPSHOT FOR TABLE
    history_log_path = 'data/history_log.json'
    if os.path.exists(history_log_path):
        with open(history_log_path, 'r') as f:
            log = json.load(f)
    else:
        log = []

    new_entry = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "spot": spot_price,
        "mstr_pain": chain_data[0]['mstr_pain'], # Track the nearest Friday
        "score": 10 if spot_price > chain_data[0]['mstr_pain'] else 5
    }

    # Only add if it's a new day
    if not log or log[-1]['date'] != new_entry['date']:
        log.append(new_entry)
        with open(history_log_path, 'w') as f:
            json.dump(log[-30:], f, indent=4) # Keep last 30 days

if __name__ == "__main__":
    update_files()
