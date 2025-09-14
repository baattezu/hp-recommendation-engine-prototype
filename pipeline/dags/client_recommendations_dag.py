from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

from tasks.load_data import load_data
from tasks.compute_signals import compute_signals
from tasks.compute_benefits import compute_products
from tasks.select_best_product import select_best_product
from tasks.generate_summary import generate_summary
from tasks.send_notification_with_mobile import send_notification
from tasks.send_notification import send_notification_to_mobile
from utils.config_loader import load_config

default_args = {
    "start_date": datetime(2025, 9, 1),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="push_reco_dag_v2",
    default_args=default_args,
    schedule="@daily",   # 👈 новое имя параметра
    catchup=False,
    max_active_runs=1,
    tags=["hackathon"]
) as dag:

    @task
    def load():
        tx, tr, clients = load_data(
            "/opt/airflow/data/client_2_transactions_3m.csv",
            "/opt/airflow/data/client_2_transfers_3m.csv",
            "/opt/airflow/data/clients.csv"
        )
        client_code = 2
        avg_monthly_balance = clients[clients['client_code'] == client_code]['avg_monthly_balance_KZT'].values[0]
        return {"transactions": tx, "transfers": tr, "avg_monthly_balance": avg_monthly_balance}

    @task
    def compute(loaded):
        signals = compute_signals(loaded)
        return signals
    
    @task
    def benefits(sigs):
        b = compute_products(sigs)
        return b

    @task
    def select_and_summary(ben, sigs):
        best = select_best_product(ben)
        product, data = best
        value = ben[product]["benefit"]
        summary = generate_summary(best)
        send_notification_to_mobile(
            client_profile={
            "client_code": 2,
            "avg_monthly_balance_KZT": 1000000,
            "fcm_token": "dummytoken"
            },
            best_product=product,
            best_value=value,
            category_spend=sigs.get("category_spend", {}),
            top3=sigs.get("top_categories", []),
            summary=summary
        )
        return summary

    @task
    def notify(summary):
        send_notification(summary)

    # задаём пайплайн (flows)
    loaded = load()
    sigs = compute(loaded)
    ben = benefits(sigs)
    summary = select_and_summary(ben, sigs)

    notify(summary)