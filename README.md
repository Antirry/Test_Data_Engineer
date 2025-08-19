# Airflow + ClickHouse: ETL для рекламы с антифродом

## Описание
Этот проект демонстрирует оркестрацию ETL-процесса в Apache Airflow для загрузки тестовых CSV-файлов с показами и кликами в ClickHouse, выполнения базовых агрегаций и запуска упрощённого антифрод-правила.

## Архитектура пайплайна
1. Extract — получение тестовых CSV (impressions.csv, clicks.csv)
2. Validate & Clean — проверка схемы, очистка мусора, дедупликация, нормализация времени в UTC
3. Load — загрузка данных в raw_impressions и raw_clicks
4. Aggregate — топ-5 кампаний по CTR за последние 30 минут
5. Fraud Check — выявление IP с >5 кликов за 10 секунд
6. Alerts — запись срабатываний в таблицу fraud_alerts

## Состав репозитория
```py
├── dags/
│   └── etl_ads.py
├── clickhouse/
│   └── ddl.sql
├── scripts/
│   └── generate_data.py
├── data/
│   ├── impressions.csv
│   └── clicks.csv
├── docker-compose.yml
└── README.md
```
## Развёртывание
### 1. Клонировать репозиторий
```bash
git clone https://github.com/Antirry/Test-Data-Engineer && cd Test-Data-Engineer
```

### 2. Запустить инфраструктуру
```bash
docker-compose up -d
```
После запуска будут доступны:
- Airflow Web UI: http://localhost:8080  (логин/пароль: admin / admin)
- ClickHouse HTTP: http://localhost:8123

## Инициализация ClickHouse
DDL-скрипт создаст необходимые таблицы:
```bash
docker exec -it clickhouse clickhouse-client --multiquery < /clickhouse/ddl.sql
```

**Структура таблиц**:
- raw_impressions / raw_clicks: события показов и кликов
- fraud_alerts: алерты антифрода

## Генерация тестовых данных
```bash
docker exec -it airflow python /scripts/generate_data.py
```
Файлы появятся в /data внутри контейнера и будут готовы к валидации/загрузке.

## Запуск DAG
1. В Airflow UI найти DAG etl_ads
2. Включить (On) и запустить (Trigger DAG)

## Проверка загрузки и агрегаций
### 1. Кол-во строк
```sql
SELECT count() FROM raw_impressions;
SELECT count() FROM raw_clicks;
```

### 2. Топ-5 кампаний по CTR
```sql
SELECT campaign_id, count() AS clicks
FROM raw_clicks
WHERE ts > now() - INTERVAL 30 MINUTE
GROUP BY campaign_id
ORDER BY clicks DESC
LIMIT 5;
```

### 3. Последние антифрод-срабатывания
```sql
SELECT * FROM fraud_alerts
WHERE ts > now() - INTERVAL 24 HOUR;
```

## Планировщик и надёжность
- Периодичность: @minutely (каждую минуту)
- Retries: 2
- Retry delay: 1 минута
- Зависимости:  
 extract → validate → load → aggregate → fraud_check

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
