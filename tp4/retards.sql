-- TP4 partie B : effet du délai de watermark sur les événements tardifs
-- Générateur : python generator.py --rate 50 --duration 300 --late-prob 0.1 --max-delay 40
-- Lancement :  docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f /opt/sql/retards.sql
-- Prérequis :  topic tp4_retards créé (1 partition), pour garder les trois résultats et
--              noter l'heure d'arrivée de chacun (Q4).
--
-- Les trois variantes ne diffèrent que par le délai du watermark : 0 s, 5 s (table commandes) et 60 s.
-- Chacune a son propre group.id pour lire le topic indépendamment.
-- Attention : dans la clause LIKE, les options sont séparées par des ESPACES. La virgule de
-- l'énoncé, (OVERWRITING WATERMARKS, OVERWRITING OPTIONS), provoque une ParseException.

CREATE TABLE commandes_w0 (WATERMARK FOR ts AS ts)
  WITH ('properties.group.id' = 'flink-w0')
  LIKE commandes (OVERWRITING WATERMARKS OVERWRITING OPTIONS);

CREATE TABLE commandes_w60 (WATERMARK FOR ts AS ts - INTERVAL '60' SECOND)
  WITH ('properties.group.id' = 'flink-w60')
  LIKE commandes (OVERWRITING WATERMARKS OVERWRITING OPTIONS);

CREATE TABLE tp4_retards (
  variante STRING,
  w_start  TIMESTAMP(3),
  w_end    TIMESTAMP(3),
  nb       BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'tp4_retards',
  'properties.bootstrap.servers' = 'kafka1:9092,kafka2:9092,kafka3:9092',
  'format' = 'json'
);

EXECUTE STATEMENT SET
BEGIN

INSERT INTO tp4_retards
SELECT 'w0', window_start, window_end, COUNT(*)
FROM TABLE(TUMBLE(TABLE commandes_w0, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;

INSERT INTO tp4_retards
SELECT 'w5', window_start, window_end, COUNT(*)
FROM TABLE(TUMBLE(TABLE commandes, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;

INSERT INTO tp4_retards
SELECT 'w60', window_start, window_end, COUNT(*)
FROM TABLE(TUMBLE(TABLE commandes_w60, DESCRIPTOR(ts), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end;

END;
