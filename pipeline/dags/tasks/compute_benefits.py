def compute_benefits(params, signals, category_spend, travel_spend, premium_spend, online_spend, top3, profile):
    benefits = {}

    total_spend = signals['total_spend']
    monthly_shortfall = max(0.0, signals['monthly_spend'] - profile['avg_monthly_balance_KZT'])
    free_balance = max(0.0, profile['avg_monthly_balance_KZT'] * 0.9)

    benefits['Карта для путешествий'] = min(params['travel_cashback_rate'] * travel_spend, params['cashback_cap'])
    benefits['Премиальная карта'] = min(params['premium_base_rate'] * total_spend +
                                        params['premium_category_bonus'] * premium_spend, params['cashback_cap'])
    benefits['Кредитная карта'] = min(params['credit_card_rate_top3'] * sum(category_spend.get(c, 0) for c in top3) +
                                      params['credit_card_online_rate'] * online_spend, params['cashback_cap'])
    benefits['Обмен валют'] = params['fx_saving_per_tx'] * signals['fx_activity']
    benefits['Кредит наличными'] = params['loan_value_buffer_rate'] * monthly_shortfall
    benefits['Депозит мультивалютный'] = free_balance * (params['deposit_multicurr_rate_annual'] / 12.0)
    benefits['Депозит сберегательный'] = profile['avg_monthly_balance_KZT'] * (params['deposit_saving_frozen_rate_annual'] / 12.0) if signals['savings_ratio'] > 0.5 else 0
    benefits['Депозит накопительный'] = profile['avg_monthly_balance_KZT'] * (params['deposit_accumulative_rate_annual'] / 12.0) * 0.4
    benefits['Инвестиции (брокерский счёт)'] = free_balance * (params['investment_expected_annual_return'] / 12.0) * 0.6
    benefits['Золотые слитки'] = free_balance * (params['gold_expected_annual_return'] / 12.0) * 0.3

    return benefits

# compute_benefits.py
# Extracted logic for computing benefit estimates for products based on signals and spends.

# def compute_benefits(params, signals, category_spend, travel_spend, premium_spend, online_spend, top3, profile):
#     """
#     Compute benefit estimates for 10 products using rule-based formulas.
    
#     Args:
#     - params: dict of business-configurable parameters
#     - signals: dict of computed signals (e.g., total_spend, savings_ratio, etc.)
#     - category_spend: dict of spend per category
#     - travel_spend: float, total spend in travel categories
#     - premium_spend: float, total spend in premium categories
#     - online_spend: float, total spend in online categories
#     - top3: list of top 3 categories by spend
#     - profile: dict of client profile (e.g., avg_monthly_balance_KZT)
    
#     Returns:
#     - benefits: dict of product to estimated benefit (KZT, monthly approx)
#     """
#     benefits = {}

#     # Precompute common values
#     total_spend = signals['total_spend']
#     monthly_shortfall = max(0.0, signals['monthly_spend'] - profile['avg_monthly_balance_KZT'])
#     free_balance = max(0.0, profile['avg_monthly_balance_KZT'] - 0.1 * profile['avg_monthly_balance_KZT'])  # assume 10% locked buffer

#     # 1. Карта для путешествий
#     benefits['Карта для путешествий'] = min(params['travel_cashback_rate'] * travel_spend, params['cashback_cap'])

#     # 2. Премиальная карта
#     base_benefit_premium = params['premium_base_rate'] * total_spend
#     category_bonus = params['premium_category_bonus'] * premium_spend
#     saved_fees = 0.0  # need fees data to estimate; set 0 in demo
#     benefits['Премиальная карта'] = min(base_benefit_premium + category_bonus + saved_fees, params['cashback_cap'])

#     # 3. Кредитная карта
#     top3_spend = sum(category_spend.get(c, 0) for c in top3)
#     credit_card_benefit = params['credit_card_rate_top3'] * top3_spend + params['credit_card_online_rate'] * online_spend
#     benefits['Кредитная карта'] = min(credit_card_benefit, params['cashback_cap'])

#     # 4. Обмен валют
#     benefits['Обмен валют'] = params['fx_saving_per_tx'] * signals['fx_activity']

#     # 5. Кредит наличными
#     benefits['Кредит наличными'] = params['loan_value_buffer_rate'] * monthly_shortfall

#     # 6. Депозит мультивалютный
#     benefits['Депозит мультивалютный'] = free_balance * (params['deposit_multicurr_rate_annual'] / 12.0)  # monthly interest approx

#     # 7. Депозит сберегательный (заморозка)
#     # Valuable if savings_ratio high and stability high
#     if signals['savings_ratio'] > 0.5 and signals['spending_stability'] > 0.6:
#         benefits['Депозит сберегательный'] = profile['avg_monthly_balance_KZT'] * (params['deposit_saving_frozen_rate_annual'] / 12.0)
#     else:
#         benefits['Депозит сберегательный'] = 0.0

#     # 8. Депозит накопительный
#     # Good for people with regular small top-ups; we don't have that signal -> small default
#     benefits['Депозит накопительный'] = profile['avg_monthly_balance_KZT'] * (params['deposit_accumulative_rate_annual'] / 12.0) * 0.4

#     # 9. Инвестиции (брокерский счёт)
#     # Value = expected return on free balance (conservative, not guaranteed)
#     benefits['Инвестиции (брокерский счёт)'] = free_balance * (params['investment_expected_annual_return'] / 12.0) * 0.6

#     # 10. Золотые слитки
#     benefits['Золотые слитки'] = free_balance * (params['gold_expected_annual_return'] / 12.0) * 0.3

#     return benefits