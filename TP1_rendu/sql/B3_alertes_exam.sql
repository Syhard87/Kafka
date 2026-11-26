-- B3_alertes_exam.sql : alertes "plus de 3 commandes dans la même minute" vers le topic alertes_exam
-- Point de départ : sql/05_alertes.sql (topic changé, règle limitée au nombre de commandes).
-- Prérequis : kafka-topics.sh --create --topic alertes_exam --partitions 3 --replication-factor 3
-- Lancement : docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f /opt/sql/B3_alertes_exam.sql

CREATE TABLE IF NOT EXISTS alertes_exam (
  customer_id STRING,
  w_start     TIMESTAMP(3),
  w_end       TIMESTAMP(3),
  nb          BIGINT,
  total       DOUBLE,
  motif       STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'alertes_exam',
  'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
  'format' = 'json',
  -- valeur par défaut du connecteur, écrite ici pour la question B4
  'sink.delivery-guarantee' = 'at-least-once'
);

INSERT INTO alertes_exam
SELECT customer_id, window_start, window_end, COUNT(*) AS nb, ROUND(SUM(amount), 2) AS total,
       'plus de 3 commandes en 1 minute' AS motif
FROM TABLE(
  TUMBLE(TABLE commandes, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
WHERE amount > 0
GROUP BY customer_id, window_start, window_end
HAVING COUNT(*) > 3;
