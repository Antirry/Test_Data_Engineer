-- Таблица для хранения показов рекламы (impressions)
CREATE TABLE raw_impressions (
    req_id UUID,                  -- ( Лучше UUID, чем строка)
    user_id UInt64,                -- (целое, без знака)
    campaign_id UInt32,
    creative_id UInt32,
    ip IPv4,                       -- (IPv4 экономит место и ускоряет фильтрацию)
    ua String,
    ts DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)        -- Партиционирование по дате события для быстрой очистки и выборок
ORDER BY (ts, req_id)              -- Сортировка по времени, затем по уникальному ID
SETTINGS index_granularity = 8192; -- Размер гранулы индекса (по умолчанию), можно настроить под объёмы

-- Таблица для хранения кликов по рекламе
CREATE TABLE raw_clicks (
    req_id UUID,                  
    user_id UInt64,               
    campaign_id UInt32,           
    creative_id UInt32,           
    ip IPv4,                      
    ua String,                    
    ts DateTime                   
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(ts)     -- Партиционирование по дате события для быстрой очистки и выборок   
ORDER BY (ts, req_id)              
SETTINGS index_granularity = 8192 -- Размер гранулы индекса (по умолчанию), можно настроить под объёмы;

-- Таблица для хранения сработок системы антифрода
CREATE TABLE fraud_alerts (
    ts DateTime,
    ip IPv4,
    clicks UInt32,
    window_start DateTime,
    window_end DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(window_start) -- Партиция по дню старта окна (можно выбрать другой критерий)
ORDER BY (window_start, ip) 
SETTINGS index_granularity = 8192 -- Размер гранулы индекса (по умолчанию), можно настроить под объёмы;
