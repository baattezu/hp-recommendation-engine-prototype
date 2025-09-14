def select_best_product(benefits):
    return max(benefits.items(), key=lambda x: x[1]["utility"])

# def select_best_products(benefits):
#     return max(benefits.items(), key=lambda x: x[10])