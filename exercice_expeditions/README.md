# Exercice intégré : suivi d'expéditions, du flux au batch

Kafka, Flink SQL, HDFS et Spark sur le cas de la Control Tower GlobalTech (KPI OTD).

## Contenu

| Dossier / fichier | Rôle |
|---|---|
| `compte-rendu/Compte_rendu_exercice_expeditions.docx` (et `.pdf`) | Compte rendu : commandes, sorties obtenues, captures, réponses aux parties 1 à 5 |
| `kit/generator_expeditions.py` | Générateur temps réel : événements CREE → EN_TRANSIT → EN_DOUANE (hors UE) → LIVRE, ~10 % d'expéditions bloquées |
| `kit/expeditions_v1.jsonl` | Historique : 1 200 expéditions (sept. à nov. 2026), 3 944 lignes avec défauts volontaires |
| `kit/creer_historique.py` | Script qui fabrique `expeditions_v1.jsonl` (graine fixe, reproductible) |
| `flink/exp_init.sql` | Table source `expeditions` (JSON, watermark 10 s, lecture depuis le début) |
| `flink/2_2_statut_par_minute.sql` | Événements par statut et par minute (tumbling) |
| `flink/2_3_expeditions_bloquees.sql` | Expéditions bloquées : session de 90 s sans LIVRE → topic `expeditions_bloquees` |
| `flink/2_4_bloquees_exactly_once.sql` | Même détection avec un sink exactly-once |
| `spark/exercice_spark.ipynb` | Notebook PySpark exécuté : bronze → silver (+ rejets) → gold, OTD |
| `hdfs/` | HDFS mono-machine en Docker (équivalent de la checklist Colab) |

## Résultats principaux

- Kafka : tous les événements d'une expédition dans une même partition ; 6 consommateurs utiles au maximum ; panne de kafka2 sans aucune erreur côté producteur.
- Flink : **10 expéditions bloquées détectées sur 100** pour chaque flux (= vérité terrain) ; 2 doublons après une panne en at-least-once, 0 en exactly-once (`read_committed`).
- HDFS : 1 bloc, réplication effective 1 (une seule machine).
- Spark : brut 3 944 = silver 3 790 + rejets 77 + doublons 77 ; 1 199 expéditions, 984 livrées ; **OTD 80,1 %** (NordFret 97,9 % … AsiaLink 71,0 %).

## Rejouer l'exercice

```bash
# Kafka et Flink : depuis le dossier du kit des TP (docker compose up …), puis
python kit/generator_expeditions.py --nb 100 --topic expeditions
# copier flink/*.sql dans kit/sql/, puis
docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/exp_init.sql -f /opt/sql/2_3_expeditions_bloquees.sql
# HDFS
cd hdfs && docker compose up -d
# Spark : ouvrir spark/exercice_spark.ipynb (variable HADOOP_USER_NAME=hadoop pour écrire dans HDFS)
```
