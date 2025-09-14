import redis
import json

def push_notification_to_stream(**context):
    r = redis.Redis(host="redis", port=6379, decode_responses=True)

    client_id = context["params"]["client_id"]
    notification = context["params"]["notification"]

    r.xadd(
        "notifications",  # stream name
        {"clientId": client_id, "payload": json.dumps(notification)}
    )
