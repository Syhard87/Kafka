# TP 1 : de l'ingestion au KPI — rendu

Dossier de rendu du TP (équivalent du dossier `NOM_Prenom_Exam1` demandé : renommez-le avant de le déposer).

| Fichier / dossier | Contenu |
|---|---|
| **`TP_1_reponses.docx`** | **l'énoncé avec, sous chaque question, la réponse : démarche, commandes, captures, explication** |
| `compte_rendu.pdf`, `compte_rendu.docx` | compte rendu complet (A à F), avec captures et réponses |
| `compte_rendu.md` | source du compte rendu |
| `presentation_TP1.pdf` | support de présentation (15 slides + 4 annexes, 16:9) ; source : `presentation/slides.html` |
| `preparation_oral.pdf` | dossier de préparation : mécanismes en profondeur et questions de la prof avec réponses justifiées |
| `notes_oral.md` | quoi dire sur chaque slide et questions probables |
| `donnees/` | `commandes_exam1.csv`, `produits_exam1.csv`, `ca_minute_flink.csv` et le script qui les génère |
| `sql/` | Flink SQL : `B1_watermarks.sql`, `B3_alertes_exam.sql`, `B4_alertes_exactly_once.sql` ; scripts `A_compression_test.py`, `B1_mesure.py`, `B4_doublons.py`, `B4_chronologie_exactly_once.py` |
| `spark/` | `TP1_spark_D_E.ipynb` (notebook exécuté) et `TP1_spark_D_E.py` (même code, en script) |
| `gold/` | couche gold en Parquet + copies CSV : CA par pays, CA par famille, indicateurs, rejets par motif |
| `lake/` | zones `curated` (commandes propres) et `rejects` (rejets avec motif), en Parquet |
| `captures/` | captures d'écran (interface Flink, terminaux, graphiques, schéma d'architecture) |
| `sorties/` | sorties texte brutes de toutes les commandes |

## Données

Les fichiers du kit examen n'étaient pas disponibles : `donnees/generer_donnees_exam1.py` génère un jeu de test au
même format, avec les anomalies décrites dans l'énoncé (graine fixe, résultat identique à chaque exécution).
**Avec les vrais fichiers, remplacez les trois CSV de `donnees/` et relancez le notebook** : les parties D et E sont
entièrement recalculées. Les chiffres des parties D et E du compte rendu devront alors être mis à jour.

## Rejouer

```
# Kafka + Flink (dossier du kit)
docker compose up -d kafka1 kafka2 kafka3 kafka-ui
docker compose --profile flink up -d --build
# copier les fichiers de sql/ dans le dossier sql/ du kit, puis par exemple :
docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f /opt/sql/B1_watermarks.sql
python generator.py --rate 50 --duration 300 --late-prob 0.1 --max-delay 40
python B1_mesure.py

# Spark (local ou Colab)
pip install pyspark pandas matplotlib
cd spark && jupyter nbconvert --to notebook --execute TP1_spark_D_E.ipynb
```
