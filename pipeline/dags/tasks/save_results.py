import pandas as pd
import os

def save_results(client_code, best_product, push_text, out_dir="outputs/"):
    os.makedirs(out_dir, exist_ok=True)
    df = pd.DataFrame([{
        "client_code": client_code,
        "product": best_product,
        "push_notification": push_text
    }])
    path = os.path.join(out_dir, f"recommendations_{client_code}.csv")
    df.to_csv(path, index=False)
    return path
