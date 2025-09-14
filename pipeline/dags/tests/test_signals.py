from tasks.compute_signals import compute_signals

def test_compute_signals_basic():
    profile = {"avg_balance": 10000}
    import pandas as pd
    df = pd.DataFrame([
        {"category": "Такси", "amount": 2000},
        {"category": "Продукты питания", "amount": 3000},
    ])
    signals = compute_signals(profile, df)
    assert signals["total_spend"] == 5000
    assert signals["travel_ratio"] > 0
