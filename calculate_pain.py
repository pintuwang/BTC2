import os
import json
import requests
from datetime import datetime, timedelta

def get_next_6_fridays():
    fridays = []
    current = datetime.now()
    # Find the next Friday
    days_ahead = (4 - current.weekday() + 7) % 7
    if days_ahead == 0: days_ahead = 7
    next_friday = current + timedelta(days=days_ahead)
    
    for i in range(6):
        target = next_friday + timedelta(weeks=i)
        # 3rd Friday logic: day must be between 15th and 21st
        is_monthly = 15 <= target.day <= 21
        fridays.append({
            "date": target.strftime("%Y-%m-%d"),
            "is_monthly": is_monthly
        })
    return fridays

def get_confidence():
    day = datetime.now().weekday()
    if day <= 1: return "Provisional (Low Confidence)"
    if day == 2: return "Sweet Spot (High Confidence)"
    return "Reactive (Maximum Gravity)"

def run_update():
    # 1. Get Live BTC Price (Deribit)
    try:
        btc_index = requests.get("https://www.deribit.com/api/v2/public/get_index_price?index_name=btc_usd").json()
        current_spot = btc_index['result']['index_price']
    except:
        current_spot = 95000.0 # Fallback

    # 2. Build 6-Week Chain with Real Expiries
    expiry_structure = get_next_6_fridays()
    chain_data = []
    
    # Simulating non-linear data for test (Replace with your specific API calls)
    for i, item in enumerate(expiry_structure):
        chain_data.append({
            "date": item['date'],
            "mstr_pain": 160 + (i * 3) + (2 if i % 2 == 0 else -1), # Non-linear MSTR
            "btc_pain": 94000 + (i * 1200) + (500 if i % 3 == 0 else -200), # Non-linear BTC
            "is_monthly": item['is_monthly']
        })

    payload = {
        "last_update_utc": datetime.utcnow().isoformat(),
        "spot": current_spot,
        "confidence": get_confidence(),
        "data": chain_data
    }

    os.makedirs('data', exist_ok=True)
    with open('data/history.json', 'w') as f:
        json.dump(payload, f, indent=4)

if __name__ == "__main__":
    run_update()
