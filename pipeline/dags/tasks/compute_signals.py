from utils.categories import TRAVEL_CATEGORIES, PREMIUM_CATEGORIES, ONLINE_CATEGORIES
import pandas as pd
# def compute_signals(client_profile, transactions):
#     total_spend = transactions['amount'].sum()
#     category_spend = transactions.groupby('category')['amount'].sum().to_dict()

#     travel_spend = sum(category_spend.get(c, 0) for c in TRAVEL_CATEGORIES)
#     premium_spend = sum(category_spend.get(c, 0) for c in PREMIUM_CATEGORIES)
#     online_spend = sum(category_spend.get(c, 0) for c in ONLINE_CATEGORIES)

#     signals = {
#         "total_spend": total_spend,
#         "savings_ratio": client_profile['avg_balance'] / (client_profile['avg_balance'] + total_spend),
#         "travel_ratio": travel_spend / total_spend if total_spend else 0,
#         "premium_ratio": premium_spend / total_spend if total_spend else 0,
#         "online_ratio": online_spend / total_spend if total_spend else 0,
#         "monthly_spend": total_spend / 3,  # упрощенно: за 3 мес
#         "spending_stability": 0.7,         # временно фикс
#         "fx_activity": 2                   # временно фикс
#     }
#     return signals, category_spend, travel_spend, premium_spend, online_spend

def compute_signals(client_data):
    """
    client_data = {
        'transactions': pd.DataFrame(...), # columns: date, category, amount, currency
        'transfers': pd.DataFrame(...),    # columns: date, type, direction, amount, currency
        'avg_monthly_balance': float
    }
    """
    transactions = client_data['transactions'].copy()
    transfers = client_data['transfers'].copy()
    avg_balance = client_data['avg_monthly_balance']

    # --- Суммы и частоты ---
    total_spend = transactions['amount'].sum()
    category_spend = transactions.groupby('category')['amount'].sum().to_dict()
    category_count = transactions['category'].value_counts().to_dict()  # сколько раз тратили в категории

    travel_spend = sum(category_spend.get(c, 0) for c in TRAVEL_CATEGORIES)
    premium_spend = sum(category_spend.get(c, 0) for c in PREMIUM_CATEGORIES)
    online_spend = sum(category_spend.get(c, 0) for c in ONLINE_CATEGORIES)

    travel_count = sum(category_count.get(c, 0) for c in TRAVEL_CATEGORIES)
    premium_count = sum(category_count.get(c, 0) for c in PREMIUM_CATEGORIES)
    online_count = sum(category_count.get(c, 0) for c in ONLINE_CATEGORIES)

    # --- Shares (доли) ---
    travel_share = travel_spend / total_spend if total_spend > 0 else 0
    premium_share = premium_spend / total_spend if total_spend > 0 else 0
    online_share = online_spend / total_spend if total_spend > 0 else 0

    # --- Расходная стабильность ---
    transactions['date'] = pd.to_datetime(transactions['date'])
    daily_spend = transactions.groupby(transactions['date'].dt.date)['amount'].sum()
    spending_stability = 1 - (daily_spend.std() / daily_spend.mean()) if not daily_spend.empty else 0

    # --- FX активность ---
    fx_ops = transfers[transfers['type'].str.contains('fx', case=False)]
    fx_activity = len(fx_ops)
    fx_share = len(fx_ops) / len(transfers) if len(transfers) > 0 else 0

    # --- Денежные потоки ---
    total_in = transfers[transfers['direction'] == 'in']['amount'].sum()
    total_out = transfers[transfers['direction'] == 'out']['amount'].sum()

    inflow_outflow_ratio = total_in / (total_out + 1)  # сколько покрываем входами
    cash_gap_ratio = (total_out - total_in) / (total_in + 1)

    # --- Savings propensity (склонность к накоплению) ---
    savings_propensity = avg_balance / (total_spend + 1)

    # --- Топ-категории ---
    top_categories = transactions['category'].value_counts().head(3).index.tolist()
    top_category = top_categories[0] if top_categories else None

    # --- Proxy-флаг: любитель роскоши / украшений ---
    JEWELRY_CATEGORIES = ["jewelry", "luxury", "boutique", "elite_restaurant"]
    jewelry_need = int(any(cat in JEWELRY_CATEGORIES for cat in top_categories))

    # --- Сигналы ---
    signals = {
        "total_spend": total_spend,
        "category_spend": category_spend,
        "category_count": category_count,

        "travel_spend": travel_spend,
        "premium_spend": premium_spend,
        "online_spend": online_spend,
        "travel_count": travel_count,
        "premium_count": premium_count,
        "online_count": online_count,

        "travel_share": travel_share,
        "premium_share": premium_share,
        "online_share": online_share,

        "monthly_spend_avg": total_spend / 1.5,  # если данные за полмесяца, делим на 1.5 мес
        "spending_stability": spending_stability,

        "fx_activity": fx_activity,
        "fx_share": fx_share,

        "inflow_outflow_ratio": inflow_outflow_ratio,
        "cash_gap_ratio": cash_gap_ratio,
        "savings_propensity": savings_propensity,

        "top_categories": top_categories,
        "top_category": top_category,
        "jewelry_need": jewelry_need,

        "avg_balance": avg_balance
    }

    return signals