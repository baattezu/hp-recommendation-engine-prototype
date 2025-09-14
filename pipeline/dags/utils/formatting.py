import math

def format_amount_kzt(x: float) -> str:
    x = float(x)
    rounded = round(x, 2)
    whole = int(math.floor(rounded))
    frac = int(round((rounded - whole) * 100))
    whole_with_sep = f"{whole:,}".replace(",", " ")
    if frac == 0:
        return f"{whole_with_sep} ₸"
    else:
        return f"{whole_with_sep},{frac:02d} ₸"
