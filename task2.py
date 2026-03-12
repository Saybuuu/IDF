import hashlib
import time
import os
import json
from functools import wraps
from dataclasses import dataclass
from typing import Optional, Dict, Any

import clickhouse_connect
import requests
from dotenv import load_dotenv

from config import MAX_RETRIES, INITIAL_DELAY, BACKOFF_FACTOR, RETRY_STATUSES, URL


load_dotenv()


class MaxRetriesExceeded(Exception):
    """
    Исключение при превышении числа попыток
    """
    pass


@dataclass
class DbConfig:
    db_host: str
    db_port: int
    db_user: str
    db_password: str
    db_name: str


def validate_env() -> DbConfig:
    """
    Проверяет обязательные переменные окружения и возвращает конфиг.
    """
    db_host = os.getenv("CLICKHOUSE_HOST")
    db_port = os.getenv("CLICKHOUSE_PORT")
    db_user = os.getenv("CLICKHOUSE_USER")
    db_password = os.getenv("CLICKHOUSE_PASSWORD")
    db_name = os.getenv("CLICKHOUSE_DB")

    missing = [
        name
        for name, value in {
            "CLICKHOUSE_HOST": db_host,
            "CLICKHOUSE_PORT": db_port,
            "CLICKHOUSE_USER": db_user,
            "CLICKHOUSE_PASSWORD": db_password,
            "CLICKHOUSE_DB": db_name,
        }.items()
        if not value
    ]

    if missing:
        raise ValueError(
            f"Отсутствуют обязательные переменные окружения: {', '.join(missing)}"
        )

    try:
        db_port = int(db_port)
    except ValueError as e:
        raise ValueError("CLICKHOUSE_PORT должен быть числом") from e

    return DbConfig(
        db_host=db_host,
        db_port=db_port,
        db_user=db_user,
        db_password=db_password,
        db_name=db_name,
    )


def handle_status_code(func):
    """
    Декоратор для обработки HTTP-ответов и повторных попыток
    при временных ошибках (сетевые сбои, статусы 429, 5xx).
    Параметры повторных попыток берутся из конфига.
    """
    @wraps(func)
    def status_code_wrapper(*args, **kwargs):
        delay = INITIAL_DELAY
        last_exception = None
        response = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = func(*args, **kwargs)

                if response.status_code == 200:
                    return response.json()

                elif response.status_code in RETRY_STATUSES:
                    print(f"Попытка {attempt}: получен статус {response.status_code}. "
                          f"Повтор через {delay:.1f} сек...")
                    time.sleep(delay)
                    delay *= BACKOFF_FACTOR
                    continue
                else:
                    print(f'Ошибка запроса (статус {response.status_code}): {response.text}')
                    return None

            except requests.exceptions.RequestException as e:
                
                print(f"Попытка {attempt}: сетевая ошибка: {e}. Повтор через {delay:.1f} сек...")
                last_exception = e
                time.sleep(delay)
                delay *= BACKOFF_FACTOR

        
        if last_exception:
            raise MaxRetriesExceeded(f"Превышено число попыток ({MAX_RETRIES}). Последняя ошибка: {last_exception}")
        else:
            raise MaxRetriesExceeded(f"Превышено число попыток ({MAX_RETRIES}). Последний статус: {response.status_code}")

    return status_code_wrapper


@handle_status_code
def get_raw_data(
    url: str,
    http_get = requests.get
) -> Optional[Dict[str, Any]]:
    """
    Получает данные по URL с возможностью подменить HTTP-клиент.
    """
    return http_get(url, timeout=10)


def get_clickhouse_client(config: DbConfig):
    return clickhouse_connect.get_client(
        host=config.db_host,
        port=config.db_port,
        username=config.db_user,
        password=config.db_password,
        database=config.db_name
    )


def ensure_table_exists(client, db_name: str) -> None:
    """
    Создаёт таблицу raw_logs, если её ещё нет.
    """
    client.command(f"""
        CREATE TABLE IF NOT EXISTS {db_name}.raw_logs (
            dedup_key String,
            raw_json String,
            created_at DateTime DEFAULT now()
        )
        ENGINE = ReplacingMergeTree(created_at)
        ORDER BY dedup_key
    """)


def build_row(raw_data: Dict[str, Any]) -> list[tuple[str, str]]:
    """
    Подготавливает строку для вставки в ClickHouse.

    - сериализует JSON
    - считает hash для дедупликации
    """

    raw_json = json.dumps(raw_data, ensure_ascii=False, sort_keys=True)

    dedup_key = hashlib.sha256(
        raw_json.encode("utf-8")
    ).hexdigest()

    return [
        (
            dedup_key,
            raw_json
        )
    ]


def insert_raw_into_db(raw_data: Dict[str, Any], config: DbConfig) -> bool:
    """
    Вставляет сырой JSON в таблицу raw_logs.
    Возвращает True при успехе, иначе False.
    """
    try:
        client = get_clickhouse_client(config)

        ensure_table_exists(
            client=client,
            db_name=config.db_name
            )

        rows = build_row(raw_data)

        # print(client.server_version)

        client.insert(
            f'{config.db_name}.raw_logs',
            rows,
            column_names=['dedup_key', 'raw_json'])

        print("Данные успешно загружены в ClickHouse")
        return True
    
    except Exception as e:
        print(f"❌ Ошибка при вставке в ClickHouse: {type(e).__name__}")
        print(f"  Сообщение: {e}")
        
        if hasattr(e, 'response') and e.response is not None:
            print(f"  Response status: {e.response.status_code}")
            print(f"  Response body: {e.response.text}")
        
        if hasattr(e, 'code'):
            print(f"  Код ошибки: {e.code}")

        return False


def main():
    try:
        config  = validate_env() 

        data = get_raw_data(url=URL)
        
        if data is not None:
            insert_raw_into_db(raw_data=data, config=config)
        else:
            print("Данные не получены (неустранимая ошибка запроса)")

    except ValueError as e:
        print(f"Ошибка конфигурации: {e}")
    except MaxRetriesExceeded as e:
        print(f"Критическая ошибка: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")


if __name__ == "__main__":
    main()