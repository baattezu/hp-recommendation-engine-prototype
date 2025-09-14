from utils.categories import TRAVEL_CATEGORIES, PREMIUM_CATEGORIES, ONLINE_CATEGORIES

def compute_signals(client_profile, transactions):
    total_spend = transactions['amount'].sum()
    category_spend = transactions.groupby('category')['amount'].sum().to_dict()

    travel_spend = sum(category_spend.get(c, 0) for c in TRAVEL_CATEGORIES)
    premium_spend = sum(category_spend.get(c, 0) for c in PREMIUM_CATEGORIES)
    online_spend = sum(category_spend.get(c, 0) for c in ONLINE_CATEGORIES)

    signals = {
        "total_spend": total_spend,
        "savings_ratio": client_profile['avg_balance'] / (client_profile['avg_balance'] + total_spend),
        "travel_ratio": travel_spend / total_spend if total_spend else 0,
        "premium_ratio": premium_spend / total_spend if total_spend else 0,
        "online_ratio": online_spend / total_spend if total_spend else 0,
        "monthly_spend": total_spend / 3,  # упрощенно: за 3 мес
        "spending_stability": 0.7,         # временно фикс
        "fx_activity": 2                   # временно фикс
    }
    return signals, category_spend, travel_spend, premium_spend, online_spend