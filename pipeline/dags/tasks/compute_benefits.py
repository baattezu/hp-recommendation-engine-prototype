import math

# --- helpers --- #

def normalize(value, max_value):
    """
    Normalizes a value to a range of 0-100.
    Handles cases where max_value is zero or negative.
    """
    if max_value <= 0:
        return 0
    # Use min to cap the score at 100
    return min(100, (value / max_value) * 100)

def make_score(benefit, usage_signal, max_value, alpha, beta, label):
    """
    Calculates the final utility score based on benefit and usage signals.
    Provides detailed logging for transparency.
    """
    if benefit <= 0:
        print(f"[{label}] No potential benefit ({benefit:.2f} KZT). Utility is 0.")
        return {"benefit": 0, "utility": 0}

    benefit_score = normalize(benefit, max_value)
    usage_score = min(usage_signal, 100)
    utility = (alpha * benefit_score) + (beta * usage_score)

    print(f"[{label}] Benefit: {benefit:.2f} KZT | Benefit Score: {benefit_score:.1f} | "
          f"Usage Score: {usage_score:.1f} | Final Utility: {utility:.1f}")
    
    return {"benefit": benefit, "utility": utility}

# --- scorers per product with explanations --- #

def score_travel_card(signals, alpha=0.9, beta=0.1): # <- Изменено: Выгода теперь важнее
    label = "Карта для путешествий"
    if 'travel_spend' not in signals:
        print(f"[{label}] Missing 'travel_spend' signal. Skipping.")
        return {"benefit": 0, "utility": 0}

    benefit = 0.04 * signals['travel_spend']
    if signals['travel_spend'] > 0:
        print(f"[{label}] Detected travel spending. Base benefit is {benefit:.2f} KZT.")
    else:
        print(f"[{label}] No travel spending detected. Benefit is 0.")

    usage_signal = signals.get('travel_count', 0)
    if usage_signal > 10:
        print(f"[{label}] Frequent travel purchases detected. High usage signal.")
    
    # <- Изменено: Увеличили max_value, чтобы повысить benefit_score для этого продукта
    return make_score(benefit, usage_signal,
                      max_value=70_000, alpha=alpha, beta=beta,
                      label=label)


def score_premium_card(signals, alpha=0.7, beta=0.3):
    label = "Премиальная карта"
    if 'total_spend' not in signals or 'avg_balance' not in signals:
        print(f"[{label}] Missing required signals 'total_spend' or 'avg_balance'. Skipping.")
        return {"benefit": 0, "utility": 0}
        
    base_cashback_rate = 0.02 if signals['avg_balance'] < 100_000 else 0.04
    premium_extra = 0.04 * signals.get('premium_spend', 0)
    benefit = base_cashback_rate * signals['total_spend'] + premium_extra
    
    print(f"[{label}] Base cashback rate is {base_cashback_rate*100:.0f}% based on avg balance.")
    if signals.get('premium_spend', 0) > 0:
        print(f"[{label}] Additional {premium_extra:.2f} KZT benefit from premium spending.")
        
    return make_score(benefit, signals.get('premium_count', 50),
                      max_value=100_000, alpha=alpha, beta=beta,
                      label=label)


def score_credit_card(signals, alpha=1.0, beta=0.0):
    label = "Кредитная карта"
    if 'category_spend' not in signals or 'online_spend' not in signals or 'total_spend' not in signals:
        print(f"[{label}] Missing signals: 'category_spend', 'online_spend', or 'total_spend'. Skipping.")
        return {"benefit": 0, "utility": 0}

    top_spend = sum(signals['category_spend'].get(cat, 0)
                    for cat in signals.get('top_categories', []))
    online_spend = signals['online_spend']
    total_spend = signals['total_spend']

    if total_spend <= 0:
        print(f"[{label}] Total spending is zero. No benefit from credit card.")
        return {"benefit": 0, "utility": 0}

    relevant_spend_share = (top_spend + online_spend) / total_spend
    relevant_spend_share = min(relevant_spend_share, 1.0)

    max_benefit_value = 80_000
    benefit = relevant_spend_share * max_benefit_value
    
    print(f"[{label}] Share of spending in relevant categories: {relevant_spend_share:.1%}. "
          f"Calculated benefit: {benefit:.2f} KZT.")
    
    return make_score(benefit, 0,
                      max_value=max_benefit_value,
                      alpha=alpha, beta=beta,
                      label=label)


def score_cash_loan(signals, alpha=0.5, beta=0.5): # <- Изменено: Уменьшили вес выгоды, увеличили вес usage
    label = "Кредит наличными"
    if 'cash_gap_ratio' not in signals:
        print(f"[{label}] Missing 'cash_gap_ratio' signal. Skipping.")
        return {"benefit": 0, "utility": 0}
        
    cash_gap = signals['cash_gap_ratio']
    
    if cash_gap <= 0.5:
        benefit = 0
        print(f"[{label}] Cash gap is too low ({cash_gap:.1%}). No benefit for a cash loan.")
    else:
        severity_multiplier = (cash_gap - 0.5) * 2
        severity_multiplier = min(severity_multiplier, 1.0)
        
        # <- Изменено: Уменьшили max_value, чтобы "утихомирить" высокий benefit
        max_benefit_value = 150_000 
        benefit = severity_multiplier * max_benefit_value

        print(f"[{label}] Significant cash gap detected ({cash_gap:.1%}). "
              f"Benefit multiplier: {severity_multiplier:.2f}.")

    return make_score(benefit, signals.get('loan_interest', 30),
                      max_value=max_benefit_value, alpha=alpha, beta=beta,
                      label=label)


def score_fx(signals, alpha=0.5, beta=0.5):
    label = "Обмен валют"
    fx_activity = signals.get('fx_activity', 0)
    if fx_activity > 0:
        benefit = 0.01 * fx_activity * 100_000
        print(f"[{label}] Detected foreign currency activity. Benefit: {benefit:.2f} KZT.")
    else:
        benefit = 0
        print(f"[{label}] No foreign currency activity. Benefit is 0.")
    
    return make_score(benefit, signals.get('fx_count', 40),
                      max_value=150_000, alpha=alpha, beta=beta,
                      label=label)


def score_savings(signals, alpha=0.8, beta=0.2):
    label = "Депозит сберегательный"
    if 'avg_balance' not in signals or signals['avg_balance'] <= 0:
        print(f"[{label}] No average balance available. Skipping.")
        return {"benefit": 0, "utility": 0}
        
    benefit = 0.03 * signals['avg_balance']
    print(f"[{label}] Benefit calculated as 3% of average balance ({signals['avg_balance']:.2f}).")
    
    return make_score(benefit, signals.get('savings_interest', 60),
                      max_value=100_000, alpha=alpha, beta=beta,
                      label=label)


def score_accumulative_deposit(signals, alpha=0.7, beta=0.3):
    label = "Депозит накопительный"
    if 'avg_balance' not in signals or signals['avg_balance'] <= 0:
        print(f"[{label}] No average balance available. Skipping.")
        return {"benefit": 0, "utility": 0}
        
    benefit = 0.025 * signals['avg_balance']
    print(f"[{label}] Benefit calculated as 2.5% of average balance ({signals['avg_balance']:.2f}).")

    return make_score(benefit, signals.get('accum_interest', 60),
                      max_value=80_000, alpha=alpha, beta=beta,
                      label=label)


def score_multi_deposit(signals, alpha=0.7, beta=0.3):
    label = "Депозит мультивалютный"
    if 'avg_balance' not in signals or signals['avg_balance'] <= 0:
        print(f"[{label}] No average balance available. Skipping.")
        return {"benefit": 0, "utility": 0}
        
    benefit = 0.02 * signals['avg_balance']
    print(f"[{label}] Benefit calculated as 2% of average balance ({signals['avg_balance']:.2f}).")
    
    return make_score(benefit, signals.get('multi_interest', 50),
                      max_value=70_000, alpha=alpha, beta=beta,
                      label=label)


def score_investments(signals, alpha=0.8, beta=0.2):
    label = "Инвестиции"
    if 'avg_balance' not in signals or signals['avg_balance'] <= 0:
        print(f"[{label}] No average balance available. Skipping.")
        return {"benefit": 0, "utility": 0}

    benefit = 0.015 * signals['avg_balance']
    print(f"[{label}] Benefit calculated as 1.5% of average balance ({signals['avg_balance']:.2f}).")

    return make_score(benefit, signals.get('invest_interest', 50),
                      max_value=60_000, alpha=alpha, beta=beta,
                      label=label)


def score_gold(signals, alpha=0.9, beta=0.1):
    label = "Золотые слитки"
    if 'avg_balance' not in signals or signals['avg_balance'] <= 0:
        print(f"[{label}] No average balance available. Skipping.")
        return {"benefit": 0, "utility": 0}

    benefit = 0.01 * signals['avg_balance']
    print(f"[{label}] Benefit calculated as 1% of average balance ({signals['avg_balance']:.2f}).")

    return make_score(benefit, signals.get('gold_interest', 40),
                      max_value=50_000, alpha=alpha, beta=beta,
                      label=label)

# --- orchestrator --- #

def compute_products(signals):
    print("\n--- Starting Product Scoring ---")
    recommendations = {
        "Карта для путешествий": score_travel_card(signals),
        "Премиальная карта": score_premium_card(signals),
        "Кредитная карта": score_credit_card(signals),
        "Кредит наличными": score_cash_loan(signals),
        "Обмен валют": score_fx(signals),
        "Депозит сберегательный": score_savings(signals),
        "Депозит накопительный": score_accumulative_deposit(signals),
        "Депозит мультивалютный": score_multi_deposit(signals),
        "Инвестиции": score_investments(signals),
        "Золотые слитки": score_gold(signals),
    }
    print("--- Finished Product Scoring ---\n")
    return recommendations

# Пример использования с вашими исходными данными:
# example_signals = {
#     'travel_spend': 437894.0,
#     'travel_count': 12,
#     'avg_balance': 100000.0,
#     'total_spend': 1255057.0,
#     'premium_spend': 100000.0,
#     'premium_count': 7,
#     'category_spend': {'top_cat_1': 300000, 'top_cat_2': 150000, 'other': 50000},
#     'online_spend': 120000,
#     'top_categories': ['top_cat_1', 'top_cat_2'],
#     'cash_gap_ratio': 0.803,
#     'loan_interest': 30,
#     'fx_activity': 0,
#     'fx_count': 0,
#     'savings_interest': 60,
#     'accum_interest': 60,
#     'multi_interest': 50,
#     'invest_interest': 50,
#     'gold_interest': 40
# }

# # Запустим систему и посмотрим на новые результаты
# results = compute_products(example_signals)
# results = sorted(results.items(), key=lambda item: item[1]['utility'], reverse=True)
# for product, data in results:
#     print(f"{product}: выгода {data['benefit']:.2f} KZT/мес | польза {data['utility']:.1f}/100")