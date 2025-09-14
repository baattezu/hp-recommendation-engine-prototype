



def transform_to_json(client_code, transactions, transfers):
    return {
        "clientId": client_code,
        "transactions": transactions.to_dict(orient="records"),
        "transfers": transfers.to_dict(orient="records"),
    }
