def send_notification(summary):
    print(f"[PUSH] {summary}")

def send_notification_to_mobile(client_profile, best_product, best_value, category_spend, top3):
    from utils.firebase import send_push_to_mobile
    from tasks.generate_summary import generate_summary

    push_text = generate_summary(
        client_profile, best_product, best_value, category_spend, top3
    )

    send_push_to_mobile(
        token=client_profile["fcm_token"],
        title="Выгодное предложение",
        body=push_text,
        data={
            "client_id": client_profile["client_id"],
            "product": best_product[0],
            "value": best_value
        }
    )