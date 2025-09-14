# Recommendation Engine Prototype

## Обзор проекта

Цель проекта — построить прототип рекомендательной системы, которая анализирует поведение клиентов и генерирует персонализированные продуктовые рекомендации вместе с push-уведомлениями.

## Подготовка окружения

Клонируйте репозиторий:
`git clone <repository-url>`
Перейдите в каталог проекта:
`cd hp-recommendation-engine-prototype`
Перенесите данные в папку pipeline/data (Airflow будет читать оттуда):
`mv data pipeline/data`

## Запуск Airflow в Docker

Перейдите в папку pipeline:
`cd pipeline`
Запустите Airflow с помощью Docker Compose:
`docker compose -f docker-compose.localexec.yaml up --build`
После сборки и запуска Airflow будет доступен по адресу:
`http://localhost:8080`

### Логин/пароль по умолчанию: airflow / airflow.

## Использование

Добавляйте датасеты в pipeline/data/.
DAG'и и скрипты пайплайнов лежат в pipeline/dags/.
Результаты сохраняются в outputs/.
