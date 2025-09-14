# Creating an informative demonstration: compute signals and benefit estimates for a single client (Айгерим)
# using the transaction snippet provided by the user. We'll:
# - parse the CSV snippet
# - compute signal features (savings_ratio, travel_ratio, premium_ratio, etc.)
# - compute benefit estimates for 10 products using rule-based formulas
# - rank products and produce a CSV output client_code,product,push_notification
# - save CSV to /mnt/data/recommendations.csv for download
# We'll use pandas and simple, explainable formulas (configurable business params).
import pandas as pd
from io import StringIO
import math
import os

csv_data = """client_code,name,product,status,city,date,category,amount,currency
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-01 09:10:36,Такси,6424.48,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-01 09:15:49,Такси,2643.9,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-01 13:10:49,Смотрим дома,4716.59,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-01 15:40:10,Такси,3669.43,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-01 17:40:30,Играем дома,5095.03,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-02 08:30:08,Смотрим дома,4043.14,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-02 08:40:51,Играем дома,5377.36,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-02 12:10:21,Продукты питания,18848.85,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 11:50:37,Такси,5541.49,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 12:10:35,Продукты питания,12139.34,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 17:00:10,Едим дома,4610.16,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 17:00:58,Кафе и рестораны,5635.01,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 17:40:05,Кафе и рестораны,5029.07,KZT
1,Айгерим,Карта для путешествий,зп,Алматы,2025-06-04 19:00:08,Кафе и рестораны,15408.79,KZT
"""

df = pd.read_csv(StringIO(csv_data), parse_dates=['date'])

# Business-configurable params (example values, should be tuned by bank)
params = {
    'travel_cashback_rate': 0.04,
    'premium_base_rate': 0.02,
    'premium_category_bonus': 0.04,  # on premium categories
    'credit_card_rate_top3': 0.10,
    'credit_card_online_rate': 0.10,
    'fx_saving_per_tx': 500.0,  # approximate KZT saved per FX event
    'loan_value_buffer_rate': 0.5,  # how much value a quick cash availability gives relative to shortfall
    'deposit_multicurr_rate_annual': 0.03,
    'deposit_saving_frozen_rate_annual': 0.06,
    'deposit_accumulative_rate_annual': 0.04,
    'investment_expected_annual_return': 0.05,
    'gold_expected_annual_return': 0.02,
    'cashback_cap': 200000.0,  # cap per period for cashback calculation (high)
}

# We'll assume some profile fields not present in CSV
profile = {
    'client_code': 1,
    'name': 'Айгерим',
    'status': 'Зарплатный клиент',
    'city': 'Алматы',
    # avg_monthly_balance_KZT is not provided in snippet; choose a reasonable assumed value for demonstration
    'avg_monthly_balance_KZT': 120_000.0
}

# Helper: categorize sets (in cyrillic as in dataset)
premium_categories = ['Ювелирные украшения', 'Косметика и Парфюмерия', 'Кафе и рестораны']
travel_categories = ['Путешествия', 'Отели', 'Такси']
online_categories = ['Едим дома', 'Смотрим дома', 'Играем дома']

# Aggregations
total_spend = df['amount'].sum()
# assume data covers 1 month (June 2025) for this demonstration
months_covered = 1.0
monthly_spend = total_spend / months_covered

# per-category spend
category_spend = df.groupby('category')['amount'].sum().to_dict()

def sum_categories(cat_list):
    return sum(category_spend.get(c, 0.0) for c in cat_list)

travel_spend = sum_categories(travel_categories)
premium_spend = sum_categories(premium_categories)
online_spend = sum_categories(online_categories)

# Signals
signals = {}
signals['total_spend'] = total_spend
signals['monthly_spend'] = monthly_spend
signals['avg_monthly_balance'] = profile['avg_monthly_balance_KZT']

# savings_ratio = avg_balance / (avg_balance + monthly_spend) -> 0..1 (higher means more saved)
signals['savings_ratio'] = profile['avg_monthly_balance_KZT'] / (profile['avg_monthly_balance_KZT'] + max(monthly_spend,1))
# spending_stability cannot be estimated reliably from 1 month -> use default medium
signals['spending_stability'] = 0.5
signals['travel_ratio'] = travel_spend / total_spend if total_spend>0 else 0.0
signals['premium_ratio'] = premium_spend / total_spend if total_spend>0 else 0.0
signals['online_ratio'] = online_spend / total_spend if total_spend>0 else 0.0
signals['fx_activity'] = 0  # no fx records in provided snippet
signals['investment_activity'] = 0
signals['credit_utilization'] = 0  # no credit info provided

# Determine top categories
top_categories = sorted(category_spend.items(), key=lambda x: x[1], reverse=True)
top3 = [c for c,_ in top_categories[:3]]

# Benefit calculations (rule-based, transparent)
benefits = {}

# 1. Карта для путешествий
benefits['Карта для путешествий'] = min(params['travel_cashback_rate'] * travel_spend, params['cashback_cap'])

# 2. Премиальная карта
base_benefit_premium = params['premium_base_rate'] * total_spend
category_bonus = params['premium_category_bonus'] * premium_spend
saved_fees = 0.0  # need fees data to estimate; set 0 in demo
benefits['Премиальная карта'] = min(base_benefit_premium + category_bonus + saved_fees, params['cashback_cap'])

# 3. Кредитная карта
top3_spend = sum(category_spend.get(c,0) for c in top3)
credit_card_benefit = params['credit_card_rate_top3'] * top3_spend + params['credit_card_online_rate'] * online_spend
benefits['Кредитная карта'] = min(credit_card_benefit, params['cashback_cap'])

# 4. Обмен валют
benefits['Обмен валют'] = params['fx_saving_per_tx'] * signals['fx_activity']

# 5. Кредит наличными
# If client has low savings_ratio relative to threshold, value of fast liquidity = shortfall * buffer_rate
monthly_shortfall = max(0.0, monthly_spend - profile['avg_monthly_balance_KZT'])
benefits['Кредит наличными'] = params['loan_value_buffer_rate'] * monthly_shortfall

# 6. Депозит мультивалютный
free_balance = max(0.0, profile['avg_monthly_balance_KZT'] - 0.1*profile['avg_monthly_balance_KZT'])  # assume 10% locked buffer
benefits['Депозит мультивалютный'] = free_balance * (params['deposit_multicurr_rate_annual'] / 12.0)  # monthly interest approx

# 7. Депозит сберегательный (заморозка)
# Valuable if savings_ratio high and stability high
if signals['savings_ratio'] > 0.5 and signals['spending_stability'] > 0.6:
    benefits['Депозит сберегательный'] = profile['avg_monthly_balance_KZT'] * (params['deposit_saving_frozen_rate_annual']/12.0)
else:
    benefits['Депозит сберегательный'] = 0.0

# 8. Депозит накопительный
# Good for people with regular small top-ups; we don't have that signal -> small default
benefits['Депозит накопительный'] = profile['avg_monthly_balance_KZT'] * (params['deposit_accumulative_rate_annual']/12.0) * 0.4

# 9. Инвестиции (брокерский счёт)
# Value = expected return on free balance (conservative, not guaranteed)
benefits['Инвестиции (брокерский счёт)'] = free_balance * (params['investment_expected_annual_return']/12.0) * 0.6

# 10. Золотые слитки
benefits['Золотые слитки'] = free_balance * (params['gold_expected_annual_return']/12.0) * 0.3

# Build DataFrame for signals and benefits
signals_df = pd.DataFrame([signals]).T.reset_index()
signals_df.columns = ['signal','value']

benefits_df = pd.DataFrame(list(benefits.items()), columns=['product','benefit_KZT']).sort_values('benefit_KZT', ascending=False).reset_index(drop=True)

# Rank and choose top product
best_product = benefits_df.iloc[0]['product']
best_value = benefits_df.iloc[0]['benefit_KZT']

# Format amount with space thousand separator and comma decimal (e.g., 27 400,50 ₸)
def format_amount_kzt(x):
    # round to nearest Tenge for clarity
    x = float(x)
    rounded = round(x, 2)
    whole = int(math.floor(rounded))
    frac = int(round((rounded - whole) * 100))
    whole_with_sep = f"{whole:,}".replace(",", " ")
    if frac == 0:
        return f"{whole_with_sep} ₸"
    else:
        return f"{whole_with_sep},{frac:02d} ₸"

# Build push notification for best product (use templates)
templates = {
    'Карта для путешествий': "{name}, в июне у вас траты на такси {taxi_sum}. С тревел-картой часть расходов вернулась бы кешбэком ≈{benefit}. Открыть карту.",
    'Премиальная карта': "{name}, у вас стабильно крупный остаток и траты в ресторанах {rest_sum}. Премиальная карта даст повышенный кешбэк и бесплатные снятия. Подключите сейчас.",
    'Кредитная карта': "{name}, ваши топ-категории — {cat1}, {cat2}, {cat3}. Кредитная карта даёт до 10% в любимых категориях. Оформить карту.",
    'Обмен валют': "{name}, вы часто платите в валюте. В приложении выгодный обмен и авто-покупка по целевому курсу. Настроить обмен.",
    'Кредит наличными': "{name}, если нужен запас на крупные траты — можно оформить кредит наличными с гибкими выплатами. Узнать доступный лимит.",
    'Депозит мультивалютный': "{name}, у вас остаются свободные средства. Разместите их на мультивалютном вкладе — удобно хранить валюту и получать процент. Открыть вклад.",
    'Депозит сберегательный': "{name}, у вас стабильный остаток. Разместите средства на сберегательном вкладе с повышенной ставкой. Открыть вклад.",
    'Депозит накопительный': "{name}, хотите копить с удобными пополнениями? Накопительный вклад поможет. Открыть вклад.",
    'Инвестиции (брокерский счёт)': "{name}, попробуйте инвестиции с низким порогом входа и без комиссий на старт. Открыть счёт.",
    'Золотые слитки': "{name}, защитите сбережения — золото может помочь в диверсификации. Посмотреть варианты."
}

taxi_sum = sum(c for k,c in category_spend.items() if 'Такси' in k)
rest_sum = category_spend.get('Кафе и рестораны', 0.0)

push_text = templates.get(best_product, "{name}, у нас есть предложение для вас. Посмотреть.")\
    .format(name=profile['name'],
            taxi_sum=format_amount_kzt(taxi_sum),
            benefit=format_amount_kzt(best_value),
            rest_sum=format_amount_kzt(rest_sum),
            cat1=top3[0] if len(top3)>0 else '',
            cat2=top3[1] if len(top3)>1 else '',
            cat3=top3[2] if len(top3)>2 else '')

# Enforce red-policy checks: length 180-220 chars preferred for push; but we'll keep concise for demo
def red_policy_ok(text):
    # checks: no ALL CAPS, <=1 exclamation, no forbidden words (simple)
    if text.upper() == text and any(c.isalpha() for c in text):  # all caps
        return False, "ALL_CAPS"
    if text.count('!') > 1:
        return False, "TOO_MANY_EXCLAMATIONS"
    if len(text) > 220:
        return False, "TOO_LONG"
    return True, ""

rp_ok, rp_reason = red_policy_ok(push_text)

# Prepare CSV output
output_df = pd.DataFrame([{
    'client_code': profile['client_code'],
    'product': best_product,
    'push_notification': push_text
}])

out_path = "mnt/data/recommendations.csv"
output_df.to_csv(out_path, index=False)

# Display results: signals, benefits, ranked products, selected push, and save CSV.
signals_df_display = signals_df.copy()
benefits_df_display = benefits_df.copy()
output_preview = output_df.copy()

signals_df_display['value'] = signals_df_display['value'].apply(lambda x: round(x,4) if isinstance(x,(float,int)) else x)

# Show some explanatory notes
notes = {
    'assumptions': [
        "avg_monthly_balance_KZT was assumed = 120 000 ₸ (not provided in snippet).",
        "Data covers a 1-month window (June 2025) in the snippet; some signals (stability) are set to defaults where insufficient history exists.",
        "Benefit formulas are rule-based, transparent, and configurable via params dict."
    ],
    'red_policy': {"ok": rp_ok, "reason": rp_reason},
    'best_product': best_product,
    'best_value_KZT': best_value
}

# Save a more detailed CSV with all benefit estimates per product for the client
detailed_out = benefits_df_display.copy()
detailed_out['benefit_KZT_formatted'] = detailed_out['benefit_KZT'].apply(format_amount_kzt)
detailed_csv_path = "mnt/data/recommendations_detailed.csv"
detailed_out.to_csv(detailed_csv_path, index=False)



# Print summary
print("Best product:", best_product)
print("Estimated benefit:", format_amount_kzt(best_value))
print("Push preview:", push_text)
print("\nFiles saved:")
print(" - Recommendations (simple):", out_path)
print(" - Recommendations (detailed):", detailed_csv_path)

# Return some python-visible objects (for notebook output)
{
    "signals": signals,
    "benefits_sorted": benefits_df.to_dict('records'),
    "push_text": push_text,
    "csv_path": out_path,
    "detailed_csv_path": detailed_csv_path,
    "notes": notes
}
