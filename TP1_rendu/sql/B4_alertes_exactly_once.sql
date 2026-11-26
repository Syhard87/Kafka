-- B4_alertes_exactly_once.sql : même job que B3, avec un puits Kafka exactly-once
-- Prérequis : kafka-topics.sh --create --topic alertes_exam_eos --partitions 3 --replication-factor 3
-- Les consommateurs doivent lire en isolation.level=read_committed pour ne voir que les transactions validées.
-- Le broker autorise des transactions d'1 h (KAFKA_TRANSACTION_MAX_TIMEOUT_MS dans docker-compose.yml).

CREATE TABLE IF NOT EXISTS alertes_exam_eos (
  customer_id STRING,
  w_start     TIMESTAMP(3),
  w_end       TIMESTAMP(3),
  nb          BIGINT,
  total       DOUBLE,
  motif       STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'alertes_exam_eos',
  'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
  'format' = 'json',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'alertes-exam-eos',
  'properties.transaction.timeout.ms' = '900000'
);

INSERT INTO alertes_exam_eos
SELECT customer_id, window_start, window_end, COUNT(*) AS nb, ROUND(SUM(amount), 2) AS total,
       'plus de 3 commandes en 1 minute' AS motif
FROM TABLE(
  TUMBLE(TABLE commandes, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
WHERE amount > 0
GROUP BY customer_id, window_start, window_end
HAVING COUNT(*) > 3;
