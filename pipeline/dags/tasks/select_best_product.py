def select_best_product(benefits):
    return max(benefits.items(), key=lambda x: x[1])