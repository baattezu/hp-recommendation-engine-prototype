def generate_summary(best_product):
    product, best_data = best_product
    return f"Лучший продукт для клиента: {product} (ожидаемая выгода {int(best_data['benefit']):,} KZT/мес)"

# def generate_summary(benefits, top_n=10):
#     # сортируем продукты по выгоде по убыванию
#     sorted_benefits = sorted(benefits.items(), key=lambda x: x[1], reverse=True)[:top_n]
#     summary_lines = []
#     for i, (product, value) in enumerate(sorted_benefits, start=1):
#         summary_lines.append(f"{i}. {product} — ожидаемая выгода {int(value):,} KZT/мес")
#     return "\n".join(summary_lines)

# def generate_summary(products, top_n=10, by="utility"):
#     """
#     Генерирует текстовый список топ-N продуктов.
    
#     by: "utility" или "benefit" — по чему сортировать
#     """
#     sorted_products = sorted(
#         products.items(),
#         key=lambda x: x[1][by],
#         reverse=True
#     )[:top_n]

#     summary_lines = []
#     for i, (product, scores) in enumerate(sorted_products, start=1):
#         benefit = int(scores["benefit"])
#         utility = round(scores["utility"], 1)
#         summary_lines.append(
#             f"{i}. {product} — выгода {benefit:,} KZT/мес | польза {utility}/100"
#         )
#     return "\n".join(summary_lines)