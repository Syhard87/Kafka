-- TP5 partie C, Q6 : salage de clé (key salting), écrit à la main
-- Générateur : python generator.py --rate 3000 --duration 0 --skew 0.8
-- Lancement :  docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f /opt/sql/skew_salage.sql
--
-- Principe : on ajoute à la clé un « sel » (ici order_id modulo 8). Le client très actif est
-- ainsi réparti sur 8 sous-clés, donc sur plusieurs sous-tâches. Une première agrégation
-- calcule des comptes partiels par (client, sel), une seconde les additionne par client.
-- Le second niveau ne reçoit que des comptes partiels : sa charge ne dépend plus du déséquilibre.
--
-- Sur une agrégation sans fenêtre, le premier niveau réémet une mise à jour à chaque message :
-- il faut le mini-batch pour qu'il n'envoie qu'un compte partiel par lot. Sur une agrégation par
-- fenêtre, le premier niveau n'émet qu'à la fermeture de la fenêtre et le mini-batch est inutile.

SET 'parallelism.default' = '4';
SET 'table.exec.mini-batch.enabled' = 'true';
SET 'table.exec.mini-batch.allow-latency' = '2 s';
SET 'table.exec.mini-batch.size' = '5000';
SET 'table.optimizer.agg-phase-strategy' = 'ONE_PHASE';

CREATE TABLE IF NOT EXISTS skew_sink (
  customer_id STRING,
  nb          BIGINT
) WITH ('connector' = 'blackhole');

INSERT INTO skew_sink
SELECT customer_id, SUM(nb_partiel) AS nb
FROM (
  SELECT customer_id, MOD(order_id, 8) AS sel, COUNT(*) AS nb_partiel
  FROM commandes
  GROUP BY customer_id, MOD(order_id, 8)
)
GROUP BY customer_id;
