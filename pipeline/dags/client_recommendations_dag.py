from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta

from tasks.load_data import load_data
from tasks.compute_signals import compute_signals
from tasks.compute_benefits import compute_benefits
from tasks.select_best_product import select_best_product
from tasks.generate_summary import generate_summary
from tasks.send_notification import send_notification
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
    tags=["hackathon"],
) as dag:

    @task
    def load():
        tx, tr = load_data(
            "/opt/airflow/data/client_50_transactions_3m.csv",
            "/opt/airflow/data/client_50_transfers_3m.csv"
        )
        return {"transactions": tx, "transfers": tr}

    @task
    def compute(loaded):
        client_profile = {"avg_balance": 1_000_000, "avg_monthly_balance_KZT": 500_000}
        signals, category_spend, travel, premium, online = compute_signals(
            client_profile, loaded["transactions"]
        )
        return {
            "signals": signals,
            "category_spend": category_spend,
            "travel": travel,
            "premium": premium,
            "online": online,
            "client_profile": client_profile,
        }

    @task
    def benefits(sigs):
        params = load_config()
        top3 = sorted(sigs["category_spend"], key=sigs["category_spend"].get, reverse=True)[:3]
        b = compute_benefits(
            params,
            sigs["signals"],
            sigs["category_spend"],
            sigs["travel"],
            sigs["premium"],
            sigs["online"],
            top3,
            sigs["client_profile"],
        )
        return b

    @task
    def select_and_summary(ben):
        best = select_best_product(ben)
        return generate_summary(best, ben)

    @task
    def notify(summary):
        send_notification(summary)

    # задаём пайплайн (flows)
    loaded = load()
    sigs = compute(loaded)
    ben = benefits(sigs)
    summary = select_and_summary(ben)
    notify(summary)