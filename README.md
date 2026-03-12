# ClickHouse + Python Test Task

Тестовое задание:

1. Поднять ClickHouse в Docker  
2. На Python реализовать загрузку данных из API в сыром виде (JSON)  
3. Реализовать дедупликацию средствами ClickHouse  
4. Сделать парсинг raw-данных через Materialized View  
5. На выходе получить таблицу `people(craft, name, _inserted_at)`

API:
http://api.open-notify.org/astros.json

---

## Стек

- Python 3
- requests
- clickhouse-connect
- Docker
- ClickHouse
- Materialized View
- ReplacingMergeTree

---

## Архитектура

Полный pipeline:
Python script
↓
task123.raw_logs (raw JSON)
↓
Materialized View
↓
task123.people (parsed data)

Вставки выполняются только в raw таблицу.  
Парсинг выполняется автоматически через Materialized View.

---

## Структура проекта
IDF/
├── click-docker/
│ ├── clickhouse/
│ │ └── init/
│ │ ├── task1.sql
│ │ └── task3.sql
│ ├── .env
│ └── docker-compose.yml
│
├── .env
├── config.py
├── task2.py
├── requirements.txt
├── README.md

### Описание файлов

| Файл | Назначение |
|------|-----------|
| docker-compose.yml | запуск ClickHouse |
| task1.sql | создание raw таблицы |
| task3.sql | создание people + MV |
| task2.py | Python загрузка данных |
| config.py | конфиг retry |
| .env | настройки подключения |
| requirements.txt | зависимости |

---

