# ETL-DAG на Apache Airflow и ClickHouse

Проект настраивает DAG, который раз в минуту: 
1. Генерирует/забирает CSV `impressions.csv`, `clicks.csv`.
2. Валидирует и очищает данные.
3. Загружает в ClickHouse (`raw_impressions`, `raw_clicks`).
4. Запускает два проверочных SQL (CTR, список IP за 24 ч) и выводит результаты в лог.
5. Применяет анти-фрод правило (>5 кликов за 10 с с одного IP) и пишет в `fraud_alerts`. ## Структура репозитория - `docker-compose.yml` — поднимает Airflow и ClickHouse. - `Dockerfile` — устанавливает зависимости для DAG. - `clickhouse/ddl.sql` — DDL для таблиц. - `queries.sql` — SQL для агрегатов. - `scripts/generate_data.py` — генератор тестовых CSV. - `dags/etl_ads.py` — основной DAG. - `data/` — монтируется в контейнер, сюда кладутся CSV. 

## Запуск 1. Клонировать репозиторий и перейти в корень:
```bash
git clone https://github.com/Antirry/Test_Data_Engineer.git cd Test_Data_Engineer
```
2. Собрать и запустить сервисы:```bash
 docker-compose up -d --build```
4. Перейти в Web UI Airflow:http://localhost:8080Логин/пароль: admin/admin
5. Проверить доступность ClickHouse:```bash
docker exec -it clickhouse clickhouse-client --query "SELECT version();"```
Как запустить DAG вручную
• В UI Airflow: найти etl_ads → Trigger DAG.
Как проверять результаты
1. Загрузка данных ```bash
docker exec -it clickhouse clickhouse-client \ --query "SELECT count() FROM default.raw_impressions;" docker exec -it clickhouse clickhouse-client \ --query "SELECT count() FROM default.raw_clicks;"```
3. Агрегации Смотрите логи таска run_aggregations в UI или в stdout контейнера:```bash
docker logs airflow```
5. Анти-фрод ```bash
docker exec -it clickhouse clickhouse-client \ --query "SELECT * FROM default.fraud_alerts LIMIT 10;"```

## Состав репозитория
```bash
├── dags/
│   └── etl_ads.py                # DAG Airflow: экстракция → валидация → загрузка → агрегаты → анти-фрод
├── clickhouse/
│   └── ddl.sql                   # SQL-скрипт создания таблиц в ClickHouse
├── scripts/
│   └── generate_data.py          # Генератор тестовых impressions.csv и clicks.csv
├── data/
│   ├── impressions.csv           # Пример входных данных (показы)
│   └── clicks.csv                # Пример входных данных (клики)
├── docker-compose.yml            # Конфигурация сервисов Airflow + ClickHouse
├── Dockerfile                    # Образ Airflow с зависимостями (clickhouse-driver, pandas)
├── queries.sql                   # SQL для проверочных агрегатов
└── README.md                     # Инструкции по запуску и проверке
```

## Теоретические заметки

<details>
<summary>PySpark</summary>
- Narrow vs Wide deps — широкие зависимости вызывают shuffle
- Shuffle — перераспределение данных; избегаем фильтрами, map-side reduce, broadcast join
- Partitioning/coalesce/repartition — управление числом партиций для оптимизации
- Broadcast join — для маленьких таблиц (<10MB), риск OOM
- UDF vs Pandas UDF — pandas UDF быстрее, т.к. vectorized через Arrow
- Caching — хранение в памяти при многократных обращениях
- Skew — salting, adaptive execution, skew join hints
- Watermark — в стриминге ограничивает "старые" данные
</details>

<details>
<summary>Hadoop</summary>
- NameNode — метаданные, DataNode — хранение блоков, Secondary NN — merge fsimage/edits
- Replication factor — надёжность vs расход диска
- Small files — объединение в контейнерные форматы или HDFS Archive
- Parquet/ORC — колонночные, Avro — строчный
- MapReduce vs Spark — Spark в памяти, DAG; MapReduce — этапы Map→Shuffle→Reduce
</details>

<details>
<summary>ClickHouse</summary>
- PRIMARY KEY vs ORDER BY — ключ поиска vs порядок хранения
- ENGINE: MergeTree, PARTITION BY toYYYYMMDD(ts), ORDER BY (campaign_id, ts)
- Replacing/Summing/Aggregating MergeTree — разные задачи (дедупликация, агрегация)
- JOIN types — ANY, ALL, SEMI, ASOF
- Materialized view — автоматическая агрегация
- TTL — удаление/перемещение старых данных
- req_id — для дедупликации событий
</details>
