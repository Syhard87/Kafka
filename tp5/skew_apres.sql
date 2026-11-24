-- TP5 partie C, version optimisée : mini-batch + agrégation en deux phases
-- Générateur : python generator.py --rate 3000 --duration 0 --skew 0.8
-- Lancement :  docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f /opt/sql/skew_apres.sql
-- Les SET doivent précéder l'INSERT : ils s'appliquent aux jobs soumis ensuite.

SET 'parallelism.default' = '4';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '2 s';
SET 'table.exec.mini-batch.size' = '5000';
SET 'table.optimizer.agg-phase-strategy' = 'TWO_PHASE';

CREATE TABLE IF NOT EXISTS skew_sink (
  customer_id STRING,
  nb          BIGINT
) WITH ('connector' = 'blackhole');

INSERT INTO skew_sink
SELECT customer_id, COUNT(*) FROM commandes GROUP BY customer_id;
