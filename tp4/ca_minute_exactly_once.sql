-- TP4 partie E : chiffre d'affaires par minute avec un sink Kafka exactly-once
-- Arrêter d'abord le job ca_minute en cours (interface Flink, Cancel Job), puis :
--   docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f /opt/sql/ca_minute_exactly_once.sql
--
-- Les messages sont écrits dans des transactions Kafka, validées à chaque checkpoint (10 s) :
-- un consommateur en read_committed ne voit que les données validées, sans doublon après reprise.
-- Le broker autorise des transactions d'une heure (KAFKA_TRANSACTION_MAX_TIMEOUT_MS dans docker-compose.yml),
-- ce qui correspond au délai de transaction par défaut du connecteur Flink.

DROP TABLE IF EXISTS ca_minute;

CREATE TABLE ca_minute (
  w_start TIMESTAMP(3),
  w_end   TIMESTAMP(3),
  nb      BIGINT,
  total   DOUBLE
) WITH (
  'connector' = 'kafka',
  'topic' = 'ca_minute',
  'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
  'format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'tp4-ca'
);

INSERT INTO ca_minute
SELECT window_start, window_end, COUNT(*) AS nb, SUM(amount) AS total
FROM TABLE(
  TUMBLE(TABLE commandes, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
WHERE amount > 0
GROUP BY window_start, window_end;
