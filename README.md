##  Задания

### Задание 1

Поднять в докере актуальную версию Clickhouse

Файл:

- task1.sql 

Особенности:

- ENGINE = ReplacingMergeTree
- дедупликация по hash
- ORDER BY dedup_key
- created_at используется как версия

---

### Задание 2

Написать Python-скрипт, который:

- получает данные из API
- обрабатывает ошибки (429, 5xx, network)
- делает retry с backoff
- вставляет JSON в ClickHouse
- не использует UPDATE/DELETE

Файл:

- task2.py 

Особенности:

- retry decorator
- exponential backoff
- проверка env переменных
- hash для дедупликации
- auto create table
- insert only

API:

http://api.open-notify.org/astros.json

---

### Задание 3

Распарсить JSON средствами ClickHouse.

Создана:

- таблица people
- materialized view
- JSONExtract
- ARRAY JOIN

Файл:

- task3.sql 

Pipeline:

raw_logs → MV → people

Используется:

- JSONExtractString
- JSONExtractArrayRaw
- ARRAY JOIN
- Materialized View

---

##  Запуск

Из директории click-docker выполнить:

docker run -d 

затем из директрии IDF выполнить: 

python task2.py