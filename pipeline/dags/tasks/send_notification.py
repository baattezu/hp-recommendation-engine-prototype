import os
import csv
import logging
import time
import json
import asyncio
from utils.firebase import send_push_to_mobile  # Предполагается, что модуль существует
from tasks.generate_summary import generate_summary  # Для обратной совместимости




OUTPUTS_DIR = "/opt/airflow/outputs"
CSV_FILE = os.path.join(OUTPUTS_DIR, "push_logs.csv")

def append_to_csv(row: dict):
    """Добавляет строку в CSV, создавая файл с заголовками при первом запуске"""
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    file_exists = os.path.isfile(CSV_FILE)

    with open(CSV_FILE, mode="a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Провайдеры: gemini (google-generativeai) или openai
PROVIDER = os.getenv("PROVIDER", "gemini").lower()

# Ленивая инициализация клиентов под выбранного провайдера
openai_client = None
gemini_model = None

def _init_provider(provider: str, model_name: str):
    global openai_client, gemini_model
    if provider == "openai":
        from openai import OpenAI
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise RuntimeError("Не найден OPENAI_API_KEY. Установите переменную окружения.")
        openai_client = OpenAI(api_key=api_key)
        return
    # provider == gemini
    import google.generativeai as genai
    api_key = 'AIzaSyBIF5n9YGGZ3imRCYvOkOPXh1koiFJY84s'
    if not api_key:
        raise RuntimeError("Не найден GOOGLE_API_KEY/GEMINI_API_KEY. Установите переменную окружения.")
    genai.configure(api_key=api_key)
    gemini_model = genai.GenerativeModel(
        model_name,
        system_instruction=SYSTEM_PROMPT,
    )

def _sanitize_push(text: str) -> str:
    """Минимальная пост-обработка: убрать кавычки по краям, ограничить длину."""
    s = text.strip()
    if (s.startswith("\"") and s.endswith("\"")) or (s.startswith("'") and s.endswith("'")):
        s = s[1:-1].strip()
    if len(s) > 220:
        s = s[:220].rstrip()
    return s

# Общие правила для обоих провайдеров
SYSTEM_PROMPT = """
Ты — генератор персонализированных пуш-уведомлений банка для канала push.
Пиши на русском (ru-KZ), живо и по делу, без клише и канцелярита.

Правила стиля и тона:
- Начинай с конкретного наблюдения по тратам/поведению.
- Польза: кратко объясни, как продукт решает задачу клиента.
- Призыв к действию: 1 краткая кнопка в конце, 2–4 слова (Открыть карту, Настроить обмен, Посмотреть условия, Открыть вклад, Узнать лимит, Оформить карту).
- Тон: на равных, доброжелательно, обращение на «вы» (с маленькой буквы), без морализаторства и давления.
- Для аудитории <25: чуть менее официально, без сленга и жаргона. Возраст не упоминать.
- Избегай пассивного залога, воды, крикливых обещаний и триггеров дефицита («успей», «только сегодня» и т.п.).
- Эмодзи: максимум одно и только если добавляет смысл. Можно без него.
- Регистр: без КАПСА. Восклицательных знаков — максимум один и только по делу.

Длина и формат:
- Длина: 180–220 символов.
- Числа: разрядность через пробел (2 499), дробная часть — через запятую (2 499,50).
- Валюта для push: символ «₸» с пробелом (2 499 ₸). Для SMS — «тг» (не использовать здесь).
- Даты: указывать только если уместны и присутствуют в данных (например, 30.09.2025).

Персонализация и данные:
- Используй только предоставленные поля клиента; ничего не выдумывай.
- Если детали отсутствуют — опусти их, сохраняя естественность.
- Если есть последняя транзакция по топ‑категории, аккуратно сослись на неё (без выдуманных сумм/дат/мерчантов).
- Если есть recommended_product — учитывай как подсказку, но конечный выбор делай по данным клиента.

Выбор продукта (ровно один релевантный оффер):
- Тревел/премиальная карта для поездок: если частые такси/перелёты/отели/путешествия; подчеркни кэшбэк/удобства в поездках.
- Премиальная карта: если стабильный высокий остаток и траты в ресторанах; упомяни повышенный кэшбэк и бесплатные снятия.
- Кредитная карта: если выраженные топ‑категории — укажи до 3 любимых категорий; обещай до 10% в любимых категориях и онлайн.
- Мультивалютный продукт: если частые платежи в иностранной валюте; «Выгодный обмен» и автопокупка по целевому курсу; CTA: «Настроить обмен».
- Вклады/сберегательные: если есть свободные средства — предложи разместить для накопления и доходности; CTA: «Открыть вклад».
- Инвестиции: предложи «низкий порог входа» и «без комиссии на старт» для осторожного знакомства.
- Кредит наличными: предлагай только при явной потребности, явном сигнале крупной траты или цели; CTA: «Узнать лимит». Иначе не предлагай.

Требование к ответу:
- Верни только текст пуш‑уведомления, без кавычек и пояснений.
- Соблюдай длину и форматирование чисел/валюты.
"""

def _gen_with_openai(model_name: str, payload: dict, attempts: int, backoff: float) -> str:
    global openai_client
    last_err = None
    for i in range(attempts):
        try:
            resp = openai_client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                ],
                temperature=0.7,
            )
            text = (resp.choices[0].message.content or "").strip()
            text = _sanitize_push(text)
            if 180 <= len(text) <= 220:
                return text
            fix = openai_client.chat.completions.create(
                model=model_name,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
                    {"role": "assistant", "content": text},
                    {"role": "user", "content": "Сохрани смысл и стиль, уложись строго в 180–220 символов."},
                ],
                temperature=0.4,
            )
            return _sanitize_push((fix.choices[0].message.content or "").strip())
        except Exception as e:
            last_err = e
            time.sleep(backoff ** i)
    raise RuntimeError(f"Не удалось сгенерировать пуш (OpenAI): {last_err}")

def _gen_with_gemini(model_name: str, payload: dict, attempts: int, backoff: float) -> str:
    global gemini_model
    last_err = None
    for i in range(attempts):
        try:
            resp = gemini_model.generate_content([json.dumps(payload, ensure_ascii=False)])
            text = (resp.text or "").strip()
            text = _sanitize_push(text)
            if 180 <= len(text) <= 220:
                return text
            resp2 = gemini_model.generate_content([
                json.dumps(payload, ensure_ascii=False),
                "Сохрани смысл и стиль, уложись строго в 180–220 символов. Верни только текст пуша."
            ])
            return _sanitize_push((resp2.text or "").strip())
        except Exception as e:
            last_err = e
            time.sleep(backoff ** i)
    raise RuntimeError(f"Не удалось сгенерировать пуш (Gemini): {last_err}")

def generate_push_with_ai(client_data: dict, attempts: int = 3, backoff: float = 1.5) -> str:
    provider = PROVIDER
    model_name = (
        os.getenv("GEMINI_MODEL", "gemini-1.5-flash") if provider == "gemini" else os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    )

    payload = {
        "channel": "push",
        "client": client_data,
    }

    _init_provider(provider, model_name)
    if provider == "gemini":
        return _gen_with_gemini(model_name, payload, attempts, backoff)
    return _gen_with_openai(model_name, payload, attempts, backoff)

def send_notification_to_mobile(client_profile, best_product, best_value, category_spend, top3, summary):
    # Подготовка данных клиента
    client_data = {
        "client_code": client_profile.get("client_code", "client_10"),
        "avg_monthly_balance_KZT": client_profile.get("avg_monthly_balance_KZT", 1000000),
        "fcm_token": client_profile.get("fcm_token", ""),
        "client_id": client_profile.get("client_id", ""),
        "best_product": best_product[0] if best_product else None,
        "best_value": best_value if best_value else 0,
        "category_spend": category_spend if category_spend else {},
        "top3": top3 if top3 else [],
        "summary": summary
    }

    # # Проверка наличия токена
    # if not client_profile.get("fcm_token"):
    #     logging.error(f"Отсутствует fcm_token для клиента {client_data['client_code']}")
    #     return

 
    # отправляем пуш
    try:
        send_push_to_mobile(client_data["fcm_token"], push_text)
        logging.info(f"Пуш отправлен клиенту {client_data['client_code']}")
    except Exception as e:
        logging.error(f"Ошибка отправки пуша: {e}")

    try:
        push_text = generate_push_with_ai(client_data)  # ждём пока вернёт
    except Exception as e:
        logging.error(f"Ошибка генерации пуша: {str(e)}")
        push_text = f"Ошибка уведомления для {client_data['client_code']}"

    try:
        # 🚀 Тут можно реальную отправку пуша вставить

        # 📊 Запись в CSV
        append_to_csv({
            "client_code": client_data["client_code"],
            "product": best_product,
            "push_text": push_text
        })

    except Exception as e:
        logging.error(f"Ошибка отправки пуша: {str(e)}")
