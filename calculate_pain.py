import os
import json
import requests
from datetime import datetime, timedelta

def get_confidence_level():
    # 0 = Monday, 2 = Wednesday, 4 = Friday
    day_num = datetime.now().weekday()
    if day_num <= 1: # Mon/Tue
        return "Provisional (Low Confidence)"
    elif day_num == 2: # Wed
        return "Sweet Spot (High Confidence)"
    else: # Thu/Fri
        return "Reactive (Maximum Gravity)"

def run_update():
    # Simulate fetching 6 Fridays
    dates = ["2025-12-26", "2026-01-02", "2026-01-09", "2026-01-16", "2026-01-23", "2026-01-30"]
    chain_data = []
    for i, date in enumerate(dates):
        chain_data.append({
            "date": date,
            "mstr_pain": 165 + (i * 2),
            "btc_pain": 96000 + (i * 500)
        })

    current_spot = 178.20 # Replace with live price fetch
    
    full_payload = {
        "last_update_utc": datetime.utcnow().isoformat(), # Use ISO UTC for JS conversion
        "spot": current_spot,
        "confidence": get_confidence_level(),
        "data": chain_data
    }

    os.makedirs('data', exist_ok=True)
    with open('data/history.json', 'w') as f:
        json.dump(full_payload, f, indent=4)

    # Historical Log Update
    log_path = 'data/history_log.json'
    if os.path.exists(log_path):
        with open(log_path, 'r') as f: log = json.load(f)
    else: log = []

    new_log = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "spot": current_spot,
        "mstr_pain": chain_data[0]['mstr_pain'],
        "confidence": full_payload["confidence"]
    }
    
    if not log or log[-1]['date'] != new_log['date']:
        log.append(new_log)
        with open(log_path, 'w') as f: json.dump(log[-30:], f, indent=4)

if __name__ == "__main__":
    run_update()
