def generate_summary(best_product, benefits):
    product, value = best_product
    return f"Лучший продукт для клиента: {product} (ожидаемая выгода {int(value):,} KZT/мес)"