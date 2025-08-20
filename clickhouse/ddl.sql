CREATE DATABASE IF NOT EXISTS default;

-- События показов: оптимизируем под агрегации по campaign_id и времени
CREATE TABLE IF NOT EXISTS raw_impressions (
  req_id String,
  user_id String,
  campaign_id String,
  creative_id String,
  ip String,
  ua String,
  ts DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (campaign_id, ts, req_id);

-- События кликов: оптимизируем под проверки по ip и времени
CREATE TABLE IF NOT EXISTS raw_clicks (
  req_id String,
  user_id String,
  campaign_id String,
  creative_id String,
  ip String,
  ua String,
  ts DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ip, ts, req_id);

CREATE TABLE IF NOT EXISTS fraud_alerts (
  ts DateTime,             -- момент фиксации правила
  ip String,
  clicks UInt32,
  window_start DateTime,
  window_end DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(ts)
ORDER BY (ip, window_start, window_end);
