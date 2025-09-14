import requests
import json

FCM_URL = "https://fcm.googleapis.com/fcm/send"
FCM_SERVER_KEY = "your_firebase_server_key"

def send_push_to_mobile(token, title, body, data=None):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"key={FCM_SERVER_KEY}"
    }
    payload = {
        "to": token,
        "notification": {
            "title": title,
            "body": body
        },
        "data": data or {}
    }

    r = requests.post(FCM_URL, headers=headers, data=json.dumps(payload))
    r.raise_for_status()
    return r.json()


# пример использования
if __name__ == "__main__":
    token = "fcmtoken_from_mobile_app"
    send_push_to_mobile(
        token,
        "Выгодное предложение",
        "Оформите премиальную карту и получите кешбэк 10% в ресторанах",
        {"product": "Премиальная карта"}
    )