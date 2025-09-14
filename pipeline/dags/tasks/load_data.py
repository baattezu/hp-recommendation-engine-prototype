import pandas as pd
from typing import Tuple

def load_data(tx_path: str, tr_path: str, clients_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    tx = pd.read_csv(tx_path)
    tr = pd.read_csv(tr_path)
    clients = pd.read_csv(clients_path)
    # небольшая валидация
    assert not tx.empty
    return tx, tr, clients