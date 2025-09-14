import yaml
import json
import os

def load_airflow_vars():
    path = "/opt/airflow/configs/airflow_variables.json"
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path) as f:
        return json.load(f)

def load_config(path=None):
    airflow_vars = load_airflow_vars()
    path = path or airflow_vars["business_params"]
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_templates(path=None):
    airflow_vars = load_airflow_vars()
    path = path or airflow_vars["templates"]
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)