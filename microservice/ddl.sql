CREATE DATABASE IF NOT EXISTS item_upload ON CLUSTER local_cluster;

CREATE TABLE IF NOT EXISTS item_upload.company_statistic 
ON CLUSTER local_cluster
    (   
        item_id Int64, 
        stat String
    ) 
engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/item_upload/company_statistic', '{replica}')
PARTITION BY intDiv(item_id, 10 * intExp10(6))
ORDER BY item_id 
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS item_upload.company_statistic_all
ON CLUSTER local_cluster
AS item_upload.company_statistic
ENGINE = Distributed(local_cluster, item_upload, company_statistic, rand());


CREATE TABLE IF NOT EXISTS item_upload.company_statistic_daily 
ON CLUSTER local_cluster
    (
        ts UInt64,
        item_id UInt64,
        company_id UInt64,
        category_id UInt64,
        origin String,
        media Map(String, UInt64),
        country Nullable(String),
        url_by_media Map(String, UInt64)
    ) 
engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/item_upload/company_statistic_daily',
                                '{replica}')
PARTITION BY toYYYYMM(toDateTime(fromUnixTimestamp64Nano(ts)))
ORDER BY (item_id, ts) 
TTL toDateTime(fromUnixTimestamp64Nano(ts)) + toIntervalDay(1) 
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS item_upload.company_statistic_daily_all
ON CLUSTER local_cluster
AS item_upload.company_statistic_daily
ENGINE = Distributed(local_cluster, item_upload, company_statistic_daily, rand());

CREATE TABLE IF NOT EXISTS item_upload.company_statistic_daily_buffer 
ON CLUSTER local_cluster
    (
        ts UInt64,
        item_id UInt64,
        company_id UInt64,
        category_id UInt64,
        origin String,
        media Map(String, UInt64),
        country Nullable(String),
        url_by_media Map(String, UInt64)
    ) engine = Buffer
    (
        'item_upload',
        'company_statistic_daily',
        1, 10, 10, 100, 1000000, 10000000, 112000000);


CREATE TABLE IF NOT EXISTS item_upload.company_statistic_status_daily 
ON CLUSTER local_cluster
    (
        ts UInt64,
        item_id UInt64,
        is_created Bool,
        up_ts UInt64 default 0
    )  
engine = ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/item_upload/company_statistic_status_daily_2',
                                    '{replica}')
PARTITION BY toYYYYMMDD(toDateTime(fromUnixTimestamp64Nano(ts)))
ORDER BY (item_id, ts) 
TTL toDateTime(fromUnixTimestamp64Nano(ts)) + toIntervalDay(1) 
SETTINGS index_granularity = 8192;


CREATE TABLE IF NOT EXISTS item_upload.company_statistic_status_daily_all
ON CLUSTER local_cluster
AS item_upload.company_statistic_status_daily
ENGINE = Distributed(local_cluster, item_upload, company_statistic_status_daily, rand());


CREATE TABLE IF NOT EXISTS item_upload.company_statistic_status_daily_buffer 
ON CLUSTER local_cluster
    (
        ts UInt64,
        item_id UInt64,
        is_created Bool,
        up_ts UInt64 default 0
    ) 
engine = Buffer
    (
        'item_upload',
        'company_statistic_status_daily',
        1, 10, 100, 10000, 10, 10000000, 112000000
    );

CREATE TABLE IF NOT EXISTS item_upload.company_statistic_monthly 
ON CLUSTER local_cluster
    (
        ts UInt64,
        company_id UInt64,
        first_time AggregateFunction(uniq, UInt64),
        media AggregateFunction(sumMap, Map(String, UInt64)),
        categories AggregateFunction(uniqMap, Map(UInt64, UInt64)),
        origins AggregateFunction(uniqMap, Map(String, UInt64)),
        country Nullable(String),
        full_time_mean AggregateFunction(avg, Float64),
        full_time_stddev AggregateFunction(stddevPop, Float64),
        attempt_time_mean AggregateFunction(avg, Float64),
        attempt_time_stddev AggregateFunction(stddevPop, Float64)
    ) 
ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/tables/{shard}/item_upload/company_statistic_monthly',
    '{replica}'
)
PARTITION BY toYYYYMMDD(toDateTime(fromUnixTimestamp64Nano(ts)))
ORDER BY (ts, company_id) 
TTL toDateTime(fromUnixTimestamp64Nano(ts)) + toIntervalMonth(1) 
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS item_upload.company_statistic_monthly_all
ON CLUSTER local_cluster
AS item_upload.company_statistic_monthly
ENGINE = Distributed(local_cluster, item_upload, company_statistic_monthly, rand());


CREATE TABLE IF NOT EXISTS item_upload.company_statistic_yearly 
ON CLUSTER local_cluster
    (
        ts UInt64,
        company_id UInt64,
        first_time AggregateFunction(sum, UInt64),
        media AggregateFunction(sumMap, Map(String, UInt64)),
        categories AggregateFunction(sumMap, Map(UInt64, UInt64)),
        origins AggregateFunction(sumMap, Map(String, UInt64)),
        country Nullable(String),
        full_time_mean AggregateFunction(avg, Float64),
        full_time_stddev AggregateFunction(stddevPop, Float64),
        attempt_time_mean AggregateFunction(avg, Float64),
        attempt_time_stddev AggregateFunction(stddevPop, Float64)
    ) 
ENGINE = ReplicatedAggregatingMergeTree(
    '/clickhouse/tables/{shard}/item_upload/company_statistic_yearly',
    '{replica}'
)
PARTITION BY toYYYYMM(toDateTime(fromUnixTimestamp64Nano(ts)))
ORDER BY (ts, company_id) 
TTL toDateTime(fromUnixTimestamp64Nano(ts)) + toIntervalYear(1) 
SETTINGS index_granularity = 8192;

CREATE TABLE IF NOT EXISTS item_upload.company_statistic_yearly_all
ON CLUSTER local_cluster
AS item_upload.company_statistic_yearly
ENGINE = Distributed(local_cluster, item_upload, company_statistic_yearly, rand());



CREATE TABLE IF NOT EXISTS item_upload.attempt_create_time 
ON CLUSTER local_cluster
    (
        date_create DateTime64(6),
        item_id UInt64,
        company_id UInt64,
        country Nullable(String),
        origin String,
        is_created Bool,
        attempt Int64,
        full_time Float64
    ) 
ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/item_upload/attempt_create_time',
    '{replica}'
)
ORDER BY (item_id, date_create);

CREATE TABLE IF NOT EXISTS item_upload.attempt_create_time_all
ON CLUSTER local_cluster
AS item_upload.attempt_create_time
ENGINE = Distributed(local_cluster, item_upload, attempt_create_time, rand());



CREATE TABLE IF NOT EXISTS item_upload.item_change_log_stat 
ON CLUSTER local_cluster
    (
        ts UInt64,
        company_id UInt64,
        country Nullable(String),
        itemOrigins AggregateFunction(uniqMap, Map(String, UInt64)),
        tryItemOrigins AggregateFunction(uniqMap, Map(String, UInt64)),
        origins AggregateFunction(uniqMap, Map(String, UInt64))
    ) 
ENGINE = AggregatingMergeTree() 
PARTITION BY toYYYYMM(toDateTime(fromUnixTimestamp64Nano(ts)))
ORDER BY (company_id, ts);

CREATE TABLE IF NOT EXISTS item_upload.item_change_log_stat_all
ON CLUSTER local_cluster
AS item_upload.item_change_log_stat
ENGINE = Distributed(local_cluster, item_upload, item_change_log_stat, rand());





CREATE MATERIALIZED VIEW IF NOT EXISTS item_upload.company_statistic_monthly_mv 
ON CLUSTER local_cluster
TO item_upload.company_statistic_yearly 
    (
        `ts` Int64,
        `company_id` UInt64,
        `country` Nullable(String),
        `first_time` AggregateFunction(sum, UInt64),
        `origins` AggregateFunction(sumMap, Map(String, UInt64)),
        `categories` AggregateFunction(sumMap, Map(UInt64, UInt64)),
        `media` AggregateFunction(sumMap, Map(String, UInt64)),
        `attempt_time_mean` AggregateFunction(avg, Float64),
        `attempt_time_stddev` AggregateFunction(stddevPop, Float64),
        `full_time_mean` AggregateFunction(avg, Float64),
        `full_time_stddev` AggregateFunction(stddevPop, Float64)
    ) AS
SELECT toUnixTimestamp64Nano(
        toDateTime64(
            toStartOfInterval(
                fromUnixTimestamp64Nano(csm.ts),
                toIntervalMonth(1)
            ),
            6
        )
    ) AS ts,
    company_id,
    country,
    sumState(finalizeAggregation(csm.first_time)) AS first_time,
    sumMapState(finalizeAggregation(csm.origins)) AS origins,
    sumMapState(finalizeAggregation(csm.categories)) AS categories,
    sumMapMergeState(csm.media) AS media,
    avgMergeState(csm.attempt_time_mean) AS attempt_time_mean,
    stddevPopMergeState(csm.attempt_time_stddev) AS attempt_time_stddev,
    avgMergeState(full_time_mean) AS full_time_mean,
    stddevPopMergeState(full_time_stddev) AS full_time_stddev
FROM item_upload.company_statistic_monthly AS csm
WHERE csm.ts >= ts
GROUP BY ts,
    company_id,
    country;

CREATE MATERIALIZED VIEW IF NOT EXISTS item_upload.company_statistic_status_daily_mv
ON CLUSTER local_cluster
TO item_upload.company_statistic_monthly
(
    `ts` Int64,
    `company_id` UInt64,
    `country` Nullable(String),
    `first_time` AggregateFunction(uniq, UInt64),
    `media` AggregateFunction(sumMap, Map(String, UInt64)),
    `categories` AggregateFunction(uniqMap, Map(UInt64, UInt64)),
    `origins` AggregateFunction(uniqMap, Map(String, UInt64)),
    `attempt_time_mean` AggregateFunction(avg, Float64),
    `attempt_time_stddev` AggregateFunction(stddevPop, Float64),
    `full_time_mean` AggregateFunction(avg, Float64),
    `full_time_stddev` AggregateFunction(stddevPop, Float64)
) AS
SELECT
    -- normalize to month-bucketed ts (start of month, in nanoseconds)
    toUnixTimestamp64Nano(
        toDateTime64(
            toStartOfInterval(fromUnixTimestamp64Nano(csd.ts), toIntervalMonth(1)),
            6
        )
    ) AS ts,

    csd.company_id,
    csd.country,

    -- count an item as 'first_time' if its daily-status row shows is_created=1
    uniqStateIf(
        toUInt64(cssd.is_created),  -- UInt64 argument
        cssd.is_created             -- filter
    ) AS first_time,

    -- roll up all the per-item media maps
    sumMapState(csd.media) AS media,

    -- build a map of category_id → unique item count
    uniqMapState(map(csd.category_id, csd.item_id)) AS categories,

    -- build a map of origin → unique item count
    uniqMapState(map(csd.origin, csd.item_id)) AS origins,

    -- average & stddev of per-item attempt time (cast to Float64 for state)
    avgState(toFloat64(act.attempt))           AS attempt_time_mean,
    stddevPopState(toFloat64(act.attempt))    AS attempt_time_stddev,

    -- average & stddev of per-item full time (cast to Float64 for state)
    avgState(toFloat64(act.full_time))        AS full_time_mean,
    stddevPopState(toFloat64(act.full_time))  AS full_time_stddev

FROM item_upload.company_statistic_daily AS csd
JOIN item_upload.company_statistic_status_daily AS cssd
    ON csd.item_id = cssd.item_id
    AND csd.ts      = cssd.ts

-- bring in the precomputed attempt/full_time per item
LEFT JOIN item_upload.attempt_create_time AS act
    ON csd.item_id = act.item_id
    -- match the day‐bucket of the daily status
    AND toDate(act.date_create) = toDateTime(fromUnixTimestamp64Nano(cssd.ts))

GROUP BY
    ts,
    csd.company_id,
    csd.country;







CREATE MATERIALIZED VIEW IF NOT EXISTS item_upload.mv_attempt_create_time 
ON CLUSTER local_cluster
TO item_upload.attempt_create_time AS
SELECT
    fromUnixTimestamp64Nano(min(csd.ts))        AS date_create,
    csd.item_id                                 AS item_id,
    csd.company_id                              AS company_id,
    csd.country                                  AS country,
    csd.origin                                  AS origin,
    any(cssd.is_created)                        AS is_created,
    if(
      any(cssd.is_created),
      ( maxIf(cssd.up_ts, cssd.is_created) - min(csd.ts) ) / 1e9,
      0
    )                                           AS attempt,
    ( maxIf(cssd.up_ts, cssd.is_created) - min(csd.ts) )/1e9 AS full_time
    
FROM item_upload.company_statistic_daily        csd
LEFT JOIN item_upload.company_statistic_status_daily cssd
  ON csd.item_id = cssd.item_id
GROUP BY
    csd.item_id,
    csd.company_id,
    csd.country,
    csd.origin;


CREATE MATERIALIZED VIEW IF NOT EXISTS item_upload.mv_item_change_log_stat
ON CLUSTER local_cluster
TO item_upload.item_change_log_stat
AS
SELECT
    toUnixTimestamp64Nano(
        toDateTime64(
            toStartOfInterval(fromUnixTimestamp64Nano(csd.ts), INTERVAL 1 DAY),
            6
        )
    ) AS ts,
    csd.company_id,
    csd.country,

    uniqMapState( map(csd.origin, csd.item_id) )                          AS origins,
    uniqMapState( if(cssd.is_created, map(csd.origin, csd.item_id), map()) )      AS itemOrigins,
    uniqMapState( if(not cssd.is_created, map(csd.origin, csd.item_id), map()) )  AS tryItemOrigins

FROM item_upload.company_statistic_status_daily AS cssd
INNER JOIN item_upload.company_statistic_daily      AS csd
    ON cssd.item_id = csd.item_id
GROUP BY
    ts,
    csd.company_id,
    csd.country
;
