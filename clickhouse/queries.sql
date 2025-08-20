-- 1) Топ-5 кампаний по CTR за последние 30 минут (корректная арифметика и сводка по окну)
WITH
  i AS (
    SELECT campaign_id, toStartOfMinute(ts) AS m, count() AS impressions
    FROM default.raw_impressions
    WHERE ts >= now() - INTERVAL 30 MINUTE
    GROUP BY campaign_id, m
  ),
  c AS (
    SELECT campaign_id, toStartOfMinute(ts) AS m, count() AS clicks
    FROM default.raw_clicks
    WHERE ts >= now() - INTERVAL 30 MINUTE
    GROUP BY campaign_id, m
  )
SELECT
  campaign_id,
  sum(clicks)      AS total_clicks,
  sum(impressions) AS total_impressions,
  toFloat64(total_clicks) / nullIf(total_impressions, 0) AS ctr
FROM (
  SELECT campaign_id, m, impressions, 0 AS clicks FROM i
  UNION ALL
  SELECT campaign_id, m, 0 AS impressions, clicks FROM c
)
GROUP BY campaign_id
ORDER BY ctr DESC
LIMIT 5;

-- 2) Список IP, попавших в fraud_alerts за последние 24 часа
SELECT DISTINCT ip
FROM default.fraud_alerts
WHERE ts >= now() - INTERVAL 24 HOUR;
