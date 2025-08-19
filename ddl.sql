CREATE TABLE raw_impressions (
    req_id String,
    user_id String,
    campaign_id String,
    creative_id String,
    ip String,
    ua String,
    ts DateTime
) ENGINE = MergeTree()
ORDER BY (campaign_id, ts);

CREATE TABLE raw_clicks AS raw_impressions;

CREATE TABLE fraud_alerts (
    ts DateTime,
    ip String,
    clicks UInt64,
    window_start DateTime,
    window_end DateTime
) ENGINE = MergeTree()
ORDER BY (ip, ts);
