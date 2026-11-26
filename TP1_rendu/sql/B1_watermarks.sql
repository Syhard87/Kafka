-- B1_watermarks.sql : CA et nombre de commandes par minute avec 3 watermarks (5 s, 30 s, 60 s)
-- Lancement : docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f /opt/sql/B1_watermarks.sql
-- Prérequis : topics ca_wm5, ca_wm30, ca_wm60 créés avec message.timestamp.type=LogAppendTime
--             (l'horodatage Kafka du résultat = heure d'émission par Flink, pour mesurer le délai).
--
-- La table "commandes" (watermark 5 s) vient de init.sql. On déclare deux autres tables sur le
-- MÊME topic, qui ne diffèrent que par le watermark (30 s et 60 s de tolérance au retard).
-- Chaque table a son propre group.id pour que les trois jobs lisent tout le flux.

CREATE TABLE IF NOT EXISTS commandes_wm30 (
  order_id    BIGINT,
  customer_id STRING,
  product     STRING,
  amount      DOUBLE,
  country     STRING,
  ts          TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '30' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'commandes',
  'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
  'properties.group.id' = 'flink-wm30',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS commandes_wm60 (
  order_id    BIGINT,
  customer_id STRING,
  product     STRING,
  amount      DOUBLE,
  country     STRING,
  ts          TIMESTAMP(3),
  WATERMARK FOR ts AS ts - INTERVAL '60' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'commandes',
  'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
  'properties.group.id' = 'flink-wm60',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- Puits : un topic par variante. emis_a = heure à laquelle Flink émet la ligne.
CREATE TABLE IF NOT EXISTS ca_wm5 (
  variante STRING, w_start TIMESTAMP(3), w_end TIMESTAMP(3), nb BIGINT, total DOUBLE, emis_a TIMESTAMP_LTZ(3)
) WITH ('connector' = 'kafka', 'topic' = 'ca_wm5',
        'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092', 'format' = 'json');

CREATE TABLE IF NOT EXISTS ca_wm30 (
  variante STRING, w_start TIMESTAMP(3), w_end TIMESTAMP(3), nb BIGINT, total DOUBLE, emis_a TIMESTAMP_LTZ(3)
) WITH ('connector' = 'kafka', 'topic' = 'ca_wm30',
        'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092', 'format' = 'json');

CREATE TABLE IF NOT EXISTS ca_wm60 (
  variante STRING, w_start TIMESTAMP(3), w_end TIMESTAMP(3), nb BIGINT, total DOUBLE, emis_a TIMESTAMP_LTZ(3)
) WITH ('connector' = 'kafka', 'topic' = 'ca_wm60',
        'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092', 'format' = 'json');

-- Trois jobs identiques, seule la source (donc le watermark) change.
-- Même règle que le job ca_minute du TP3 : on ne compte que les montants > 0.
INSERT INTO ca_wm5
SELECT 'wm5', window_start, window_end, COUNT(*), ROUND(SUM(amount), 2), CURRENT_TIMESTAMP
FROM TABLE(TUMBLE(TABLE commandes, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
WHERE amount > 0
GROUP BY window_start, window_end;

INSERT INTO ca_wm30
SELECT 'wm30', window_start, window_end, COUNT(*), ROUND(SUM(amount), 2), CURRENT_TIMESTAMP
FROM TABLE(TUMBLE(TABLE commandes_wm30, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
WHERE amount > 0
GROUP BY window_start, window_end;

INSERT INTO ca_wm60
SELECT 'wm60', window_start, window_end, COUNT(*), ROUND(SUM(amount), 2), CURRENT_TIMESTAMP
FROM TABLE(TUMBLE(TABLE commandes_wm60, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
WHERE amount > 0
GROUP BY window_start, window_end;
