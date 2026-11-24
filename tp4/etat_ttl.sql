-- TP4 partie C (étape 3) : même agrégation sans fenêtre, avec un TTL sur l'état
-- Générateur : python generator.py --rate 500 --duration 0 --customers 200000
-- Lancement :  docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f /opt/sql/etat_ttl.sql
-- L'état d'un client non mis à jour depuis 10 minutes est supprimé (son compteur repart de zéro
-- s'il revient). Pour observer le plafond en quelques minutes, on peut tester avec '1 min'.

SET 'table.exec.state.ttl' = '10 min';

CREATE TABLE IF NOT EXISTS etat_sink (
  customer_id STRING,
  nb          BIGINT,
  total       DOUBLE
) WITH ('connector' = 'blackhole');

INSERT INTO etat_sink
SELECT customer_id, COUNT(*), SUM(amount)
FROM commandes
GROUP BY customer_id;
