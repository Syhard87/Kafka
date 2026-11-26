# %% [markdown]
# # TP 1 : de l'ingestion au KPI — Parties D (Spark) et E (KPI et réconciliation)
#
# Notebook PySpark. Il tourne tel quel en local ou sur Google Colab.
#
# **Sur Colab** : `!pip install pyspark`, déposez les trois CSV dans `/content/donnees`, puis mettez
# `DATA_DIR = "/content/donnees"`. Pour lire depuis HDFS (partie C), remplacez les chemins par
# `hdfs://localhost:9000/data/raw/...` (adresse donnée par la checklist Colab).
#
# **Données** : les fichiers du kit examen n'étaient pas disponibles. Le jeu utilisé est généré par
# `donnees/generer_donnees_exam1.py` (même format, mêmes types d'anomalies, graine fixe). Avec les vrais
# fichiers, il suffit de remplacer les trois CSV et de relancer toutes les cellules.

# %%
import json
import os
import time
import urllib.request

import matplotlib
import matplotlib.ticker
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

DATA_DIR = os.environ.get("DATA_DIR", "../donnees")    # CSV du kit
LAKE_DIR = os.environ.get("LAKE_DIR", "../lake")       # zones curated / rejects (comme dans HDFS)
GOLD_DIR = os.environ.get("GOLD_DIR", "../gold")       # couche gold (= /data/analytics)
FIG_DIR = os.environ.get("FIG_DIR", "../captures")

spark = (SparkSession.builder.master("local[4]").appName("TP1_examen")
         # dates invalides -> NULL au lieu d'une erreur (comportement identique Spark 3.5 et 4.x)
         .config("spark.sql.ansi.enabled", "false")
         .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.showConsoleProgress", "false")
         .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
pd.set_option("display.width", 200)
print("Spark", spark.version)

# %% [markdown]
# ## D1. Contrôle qualité en un seul `agg`
#
# Toutes les colonnes sont lues en texte (schéma imposé) : on contrôle nous-mêmes les conversions,
# sinon une date invalide ou une quantité illisible disparaîtrait sans laisser de trace.

# %%
SCHEMA_CMD = ("id_commande STRING, id_entrepot STRING, id_produit STRING, pays STRING, quantite STRING, "
              "prix_unitaire STRING, ts_evenement STRING, ts_ingestion STRING")
TS_FMT = "yyyy-MM-dd HH:mm:ss.SSS"
COLONNES = ["id_commande", "id_entrepot", "id_produit", "pays", "quantite", "prix_unitaire",
            "ts_evenement", "ts_ingestion"]

brut = spark.read.csv(f"{DATA_DIR}/commandes_exam1.csv", header=True, schema=SCHEMA_CMD)

cmd = (brut
       .withColumn("quantite_i", F.col("quantite").cast("int"))
       .withColumn("prix", F.col("prix_unitaire").cast("double"))
       .withColumn("ts_evt", F.to_timestamp("ts_evenement", TS_FMT))
       .withColumn("ts_ing", F.to_timestamp("ts_ingestion", TS_FMT)))

qualite = cmd.agg(
    F.count("*").alias("nb_lignes"),
    F.countDistinct("id_commande").alias("nb_id_commande_distincts"),
    (F.count("*") - F.countDistinct("id_commande")).alias("doublons_id_commande"),
    *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in COLONNES],
    F.sum((F.col("quantite_i") <= 0).cast("int")).alias("quantite_<=0"),
    F.sum((F.col("ts_evenement").isNotNull() & F.col("ts_evt").isNull()).cast("int")).alias("dates_invalides"),
)
q = qualite.first().asDict()
tableau_d1 = pd.DataFrame({"contrôle": list(q), "valeur": list(q.values())})
tableau_d1["% des lignes"] = (100 * tableau_d1["valeur"] / q["nb_lignes"]).round(2)
print(tableau_d1.to_string(index=False))

# %% [markdown]
# Les dates invalides sont celles qui sont renseignées mais illisibles ; les dates vides sont comptées
# dans `null_ts_evenement`. Exemples :

# %%
cmd.filter(F.col("ts_evt").isNull()).groupBy("ts_evenement").count().orderBy(F.desc("count")).show(truncate=False)

# %% [markdown]
# ## D2. Pipeline propre : dédoublonnage, motif de rejet, deux sorties Parquet
#
# * **Dédoublonnage** sur `id_commande` : on garde la **première arrivée** (plus petit `ts_ingestion`).
#   Les copies sont des renvois du producteur, identiques à l'original sauf l'heure d'ingestion.
# * **Motif de rejet** : un seul motif par ligne, dans l'ordre de l'énoncé
#   (`pays_absent` > `quantite_invalide` > `prix_absent` > `date_invalide`). Une date vide est traitée
#   comme une date invalide.

# %%
premiere_arrivee = Window.partitionBy("id_commande").orderBy(F.col("ts_ing").asc_nulls_last(), "ts_ingestion")

MOTIF = (F.when(F.col("pays").isNull() | (F.trim("pays") == ""), "pays_absent")
          .when(F.col("quantite_i").isNull() | (F.col("quantite_i") <= 0), "quantite_invalide")
          .when(F.col("prix").isNull(), "prix_absent")
          .when(F.col("ts_evt").isNull(), "date_invalide"))

# toutes les lignes brutes, avec leur rang d'arrivée et leur motif (réutilisé en E2)
lignes = (cmd.withColumn("rang_arrivee", F.row_number().over(premiere_arrivee))
             .withColumn("motif_rejet", MOTIF)
             .withColumn("montant", F.round(F.col("quantite_i") * F.col("prix"), 2)))
lignes.cache()

distinctes = lignes.filter("rang_arrivee = 1")

propres = (distinctes.filter(F.col("motif_rejet").isNull())
           .select("id_commande", "id_entrepot", "id_produit", "pays",
                   F.col("quantite_i").alias("quantite"), F.col("prix").alias("prix_unitaire"),
                   F.col("montant"), F.col("ts_evt").alias("ts_evenement"), F.col("ts_ing").alias("ts_ingestion")))

rejets = (distinctes.filter(F.col("motif_rejet").isNotNull())
          .select(*COLONNES, "motif_rejet", F.current_timestamp().alias("ts_rejet")))

# Peu de lignes : 1 fichier par sortie, pour ne pas fabriquer de petits fichiers (cf. partie C)
propres.coalesce(1).write.mode("overwrite").parquet(f"{LAKE_DIR}/curated/commandes")
rejets.coalesce(1).write.mode("overwrite").parquet(f"{LAKE_DIR}/rejects/commandes")

# %% [markdown]
# **Preuve** : on relit les deux dossiers Parquet écrits et on compare à `countDistinct(id_commande)`
# calculé sur le fichier brut.

# %%
propres_pq = spark.read.parquet(f"{LAKE_DIR}/curated/commandes")
rejets_pq = spark.read.parquet(f"{LAKE_DIR}/rejects/commandes")
nb_propres, nb_rejets = propres_pq.count(), rejets_pq.count()
nb_distinctes = brut.select("id_commande").distinct().count()
print(f"propres ({nb_propres}) + rejets ({nb_rejets}) = {nb_propres + nb_rejets}")
print(f"commandes distinctes                 = {nb_distinctes}")
assert nb_propres + nb_rejets == nb_distinctes
print("OK : propres + rejets = commandes distinctes")
print("Aucune commande en double après dédoublonnage :",
      propres_pq.groupBy("id_commande").count().filter("count > 1").count() == 0)
rejets_pq.groupBy("motif_rejet").count().orderBy(F.desc("count")).show()

# %% [markdown]
# Lignes qui cumulent plusieurs anomalies (elles ne reçoivent que le premier motif) :

# %%
nb_anomalies = (F.when(F.col("pays").isNull(), 1).otherwise(0)
                + F.when(F.col("quantite_i").isNull() | (F.col("quantite_i") <= 0), 1).otherwise(0)
                + F.when(F.col("prix").isNull(), 1).otherwise(0)
                + F.when(F.col("ts_evt").isNull(), 1).otherwise(0))
print("commandes distinctes avec au moins 2 anomalies :",
      distinctes.filter(nb_anomalies >= 2).count())

# %% [markdown]
# ## D3. Jointure avec les produits et skew
#
# On désactive le broadcast automatique et l'AQE pour voir la jointure « brute » que Spark ferait
# entre deux grosses tables.

# %%
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.conf.set("spark.sql.adaptive.enabled", "false")

produits = spark.read.csv(f"{DATA_DIR}/produits_exam1.csv", header=True,
                          schema="id_produit STRING, nom_produit STRING, famille STRING, prix_catalogue DOUBLE")

ca_famille = (propres_pq.join(produits, "id_produit")
              .groupBy("famille")
              .agg(F.round(F.sum("montant"), 2).alias("ca"), F.count("*").alias("nb_commandes"))
              .orderBy(F.desc("ca")))


def plan_physique(df):
    return df._jdf.queryExecution().executedPlan().toString()


def resume_plan(df):
    p = plan_physique(df)
    jointure = next((j for j in ("BroadcastHashJoin", "SortMergeJoin", "ShuffledHashJoin") if j in p), "?")
    return {"jointure": jointure,
            "Exchange (shuffle)": p.count("Exchange hashpartitioning") + p.count("Exchange rangepartitioning")
                                  + p.count("Exchange SinglePartition"),
            "BroadcastExchange": p.count("BroadcastExchange")}


ca_famille.explain()
print(resume_plan(ca_famille))
ca_famille.show()

# %% [markdown]
# **Répartition des lignes par `id_produit`** :

# %%
total_propres = propres_pq.count()
par_produit = (propres_pq.groupBy("id_produit").count()
               .withColumn("part_%", F.round(100 * F.col("count") / total_propres, 2))
               .orderBy(F.desc("count")))
par_produit.show(5)
stats_prod = par_produit.agg(F.max("count").alias("max"), F.avg("count").alias("moyenne"),
                             F.expr("percentile_approx(count, 0.5)").alias("mediane")).first()
print(f"{par_produit.count()} produits ; max = {stats_prod['max']} lignes, "
      f"moyenne = {stats_prod['moyenne']:.0f}, médiane = {stats_prod['mediane']}")

# %% [markdown]
# **Répartition par partition après `repartition('id_produit')`** (200 partitions de shuffle par défaut) :

# %%
NB_PARTITIONS_SHUFFLE = int(spark.conf.get("spark.sql.shuffle.partitions"))
reparti = propres_pq.repartition("id_produit")
par_partition = (reparti.groupBy(F.spark_partition_id().alias("partition"))
                 .agg(F.count("*").alias("lignes"), F.collect_set("id_produit").alias("produits"))
                 .orderBy(F.desc("lignes")))
par_partition.show(6, truncate=60)
pp = par_partition.toPandas()
non_vides = len(pp)
moy_non_vides = pp["lignes"].mean()
max_part = pp["lignes"].max()
mediane_part = pp["lignes"].median()
print(f"{NB_PARTITIONS_SHUFFLE} partitions, dont {non_vides} non vides et {NB_PARTITIONS_SHUFFLE - non_vides} vides")
print(f"plus grosse partition : {max_part} lignes = {100 * max_part / total_propres:.1f} % des lignes")
print(f"rapport max / moyenne des partitions non vides : {max_part / moy_non_vides:.1f}")
print(f"rapport max / médiane des partitions non vides : {max_part / mediane_part:.1f}")

fig, ax = plt.subplots(figsize=(9, 3.2))
ax.bar(range(non_vides), pp["lignes"], color=["#d1495b"] + ["#4c78a8"] * (non_vides - 1))
ax.axhline(moy_non_vides, color="#333", ls="--", lw=1, label=f"moyenne ({moy_non_vides:.0f})")
ax.set_xlabel("partitions non vides (triées)")
ax.set_ylabel("lignes")
ax.set_title("Lignes par partition après repartition('id_produit')")
ax.legend()
fig.tight_layout()
fig.savefig(f"{FIG_DIR}/D3_skew_partitions.png", dpi=150)

# %% [markdown]
# ## D4. Correction : jointure en broadcast
#
# Le jeu de test est petit : pour que les durées soient mesurables, on mesure aussi sur une copie
# ×100 des commandes propres (même distribution, donc même skew), répartie en 8 fichiers.
# Les métriques par tâche viennent de l'API REST de l'interface Spark (port 4040) : on regarde
# l'étape qui exécute la jointure et le nombre de lignes traitées par chacune de ses tâches.

# %%
FACTEUR = 100
(propres_pq.withColumn("copie", F.explode(F.sequence(F.lit(1), F.lit(FACTEUR))))
 .repartition(8).write.mode("overwrite").parquet(f"{LAKE_DIR}/bench/commandes_x{FACTEUR}"))
gros = spark.read.parquet(f"{LAKE_DIR}/bench/commandes_x{FACTEUR}")
N_GROS = gros.count()
print("lignes du jeu de mesure :", N_GROS)

API = spark.sparkContext.uiWebUrl + "/api/v1/applications/" + spark.sparkContext.applicationId


def api(chemin):
    with urllib.request.urlopen(API + chemin) as r:
        return json.loads(r.read())


def etape_de_jointure(groupe):
    """Dernière étape du job qui traite (presque) toutes les commandes : c'est là qu'a lieu la jointure."""
    jobs = [j for j in api("/jobs") if j.get("jobGroup") == groupe]
    candidates = []
    for s in sorted({s for j in jobs for s in j["stageIds"]}):
        for t in api(f"/stages/{s}"):
            if t["status"] == "COMPLETE" and t["inputRecords"] + t["shuffleReadRecords"] >= 0.9 * N_GROS:
                candidates.append((s, t["attemptId"]))
    return candidates[-1]


def mesurer(nom, construire, repetitions=3):
    construire().collect()                                   # échauffement (non mesuré)
    durees = []
    for i in range(repetitions):
        groupe = f"{nom}-{i}"
        spark.sparkContext.setJobGroup(groupe, groupe)
        t0 = time.perf_counter()
        construire().collect()
        durees.append(time.perf_counter() - t0)
    s, a = etape_de_jointure(groupe)
    taches = api(f"/stages/{s}/{a}/taskList?length=10000")
    lignes = sorted(t["taskMetrics"]["inputMetrics"]["recordsRead"]
                    + t["taskMetrics"]["shuffleReadMetrics"]["recordsRead"] for t in taches)
    pleines = [n for n in lignes if n > 0]
    duree_taches = [t["taskMetrics"]["executorRunTime"] for t in taches]
    lignes_shuffle = sum(t["shuffleWriteRecords"] for j in api("/jobs") if j.get("jobGroup") == groupe
                         for st in j["stageIds"] for t in api(f"/stages/{st}") if t["status"] == "COMPLETE")
    return {"variante": nom, **resume_plan(construire()),
            "durée moyenne (s)": round(sum(durees) / len(durees), 2),
            "lignes écrites en shuffle": lignes_shuffle,
            "tâches de l'étape de jointure": len(taches),
            "  dont tâches non vides": len(pleines),
            "lignes de la tâche la plus chargée": max(pleines),
            "lignes de la tâche médiane (non vides)": int(pd.Series(pleines).median()),
            "déséquilibre max / médiane": round(max(pleines) / pd.Series(pleines).median(), 1),
            "durée de la tâche la plus longue (ms)": max(duree_taches)}


def ca_famille_smj():
    return gros.join(produits, "id_produit").groupBy("famille").agg(F.sum("montant").alias("ca"))


def ca_famille_broadcast():
    return gros.join(F.broadcast(produits), "id_produit").groupBy("famille").agg(F.sum("montant").alias("ca"))


avant = mesurer("avant (SortMergeJoin)", ca_famille_smj)
apres = mesurer("après (broadcast)", ca_famille_broadcast)
mesures_d4 = pd.DataFrame([avant, apres]).set_index("variante").T
print(mesures_d4.to_string())

# %% [markdown]
# Plan après correction (le côté commandes n'est plus redistribué : chaque tâche joint ses lignes
# avec la copie locale des 40 produits) :

# %%
ca_famille_broadcast().explain()

# %% [markdown]
# Vérification : les deux versions donnent exactement le même chiffre d'affaires par famille.

# %%
a = ca_famille_smj().withColumnRenamed("ca", "ca_smj")
b = ca_famille_broadcast().withColumnRenamed("ca", "ca_bc")
print("écart max entre les deux versions :",
      a.join(b, "famille").select(F.max(F.abs(F.col("ca_smj") - F.col("ca_bc")))).first()[0])
spark.sparkContext.setJobGroup("e", "e")
spark.conf.set("spark.sql.adaptive.enabled", "true")       # on remet les réglages par défaut
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

# %% [markdown]
# ## E1. Couche gold (Parquet)

# %%
ca_total = propres_pq.agg(F.sum("montant")).first()[0]

gold_ca_pays = (propres_pq.groupBy("pays")
                .agg(F.round(F.sum("montant"), 2).alias("ca"), F.count("*").alias("nb_commandes"))
                .withColumn("panier_moyen", F.round(F.col("ca") / F.col("nb_commandes"), 2))
                .withColumn("part_ca_pct", F.round(100 * F.col("ca") / ca_total, 2))
                .orderBy(F.desc("ca")))

gold_ca_famille = (propres_pq.join(F.broadcast(produits), "id_produit")
                   .groupBy("famille")
                   .agg(F.round(F.sum("montant"), 2).alias("ca"), F.count("*").alias("nb_commandes"),
                        F.sum("quantite").alias("quantite_totale"))
                   .withColumn("part_ca_pct", F.round(100 * F.col("ca") / ca_total, 2))
                   .orderBy(F.desc("ca")))

nb_lignes_brutes = q["nb_lignes"]
gold_indicateurs = spark.createDataFrame([{
    "date_donnees": "2026-11-02",
    "nb_lignes_brutes": nb_lignes_brutes,
    "nb_doublons": nb_lignes_brutes - nb_distinctes,
    "nb_commandes_distinctes": nb_distinctes,
    "nb_commandes_propres": nb_propres,
    "nb_rejets": nb_rejets,
    "taux_rejet_pct": round(100 * nb_rejets / nb_distinctes, 2),
    "ca_total": round(ca_total, 2),
}]).select("date_donnees", "nb_lignes_brutes", "nb_doublons", "nb_commandes_distinctes",
           "nb_commandes_propres", "nb_rejets", "taux_rejet_pct", "ca_total")

gold_rejets_motif = (rejets_pq.groupBy("motif_rejet").count().withColumnRenamed("count", "nb_rejets")
                     .withColumn("part_des_rejets_pct", F.round(100 * F.col("nb_rejets") / nb_rejets, 2))
                     .orderBy(F.desc("nb_rejets")))

for nom, df in [("ca_par_pays", gold_ca_pays), ("ca_par_famille", gold_ca_famille),
                ("indicateurs", gold_indicateurs), ("rejets_par_motif", gold_rejets_motif)]:
    df.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_DIR}/{nom}")
    df.toPandas().to_csv(f"{GOLD_DIR}/{nom}.csv", index=False)     # copie lisible pour le rendu
    print(f"== gold/{nom}")
    df.show(truncate=False)

# %% [markdown]
# ## E2. Réconciliation Flink / Spark
#
# **Méthode.** On rejoue, ligne par ligne sur les données brutes (doublons compris), les règles du job
# Flink décrites dans l'énoncé, et on les compare à nos règles Spark :
#
# * `compte_flink` : quantité > 0, prix renseigné, date lisible (sinon pas de fenêtre), et
#   `ts_ingestion` ≤ fin de la minute + 5 s ; pas de dédoublonnage, pas de contrôle du pays.
# * `compte_spark` : première arrivée de la commande et aucun motif de rejet.
#
# Chaque ligne contribue `montant × (compte_flink − compte_spark)` à l'écart. Une ligne comptée d'un
# seul côté l'est pour une seule raison, ce qui donne une décomposition exacte.
#
# D'abord, un contrôle : ces règles rejouées doivent redonner le fichier `ca_minute_flink.csv`.

# %%
flink = (spark.read.csv(f"{DATA_DIR}/ca_minute_flink.csv", header=True,
                        schema="w_start STRING, w_end STRING, nb LONG, total DOUBLE")
         .withColumn("w_start", F.to_timestamp("w_start", "yyyy-MM-dd HH:mm:ss")))

fin_minute = F.date_trunc("minute", "ts_evt") + F.expr("INTERVAL 1 MINUTE")
lignes_e2 = (lignes
             .withColumn("minute", F.date_trunc("minute", "ts_evt"))
             .withColumn("compte_flink", F.coalesce(
                 (F.col("quantite_i") > 0) & F.col("prix").isNotNull() & F.col("ts_evt").isNotNull()
                 & (F.col("ts_ing") <= fin_minute + F.expr("INTERVAL 5 SECONDS")), F.lit(False)))
             .withColumn("compte_spark", (F.col("rang_arrivee") == 1) & F.col("motif_rejet").isNull())
             .withColumn("retard_s", F.col("ts_ing").cast("double") - fin_minute.cast("double")))

rejoue = (lignes_e2.filter("compte_flink").groupBy("minute")
          .agg(F.count("*").alias("nb_rejoue"), F.sum(F.col("quantite_i") * F.col("prix")).alias("total_rejoue")))
controle = flink.join(rejoue, flink.w_start == rejoue.minute, "full_outer")
controle.select(F.count("*").alias("minutes"),
                F.sum((F.col("nb") != F.col("nb_rejoue")).cast("int")).alias("minutes_nb_different"),
                F.round(F.max(F.abs(F.col("total") - F.col("total_rejoue"))), 4).alias("ecart_max_par_minute")).show()

# %%
ca_flink = flink.agg(F.sum("total")).first()[0]
ca_spark = ca_total
ecart = ca_flink - ca_spark

CAUSE = (F.when(F.col("compte_flink") & ~F.col("compte_spark") & (F.col("rang_arrivee") > 1),
                "1. doublons comptés par Flink")
          .when(F.col("compte_flink") & ~F.col("compte_spark") & F.col("pays").isNull(),
                "2. pays absent compté par Flink")
          .when(~F.col("compte_flink") & F.col("compte_spark"),
                "3. événements tardifs ignorés par Flink"))
contribution = (F.col("compte_flink").cast("int") - F.col("compte_spark").cast("int")) \
    * F.col("quantite_i") * F.col("prix")

causes = (lignes_e2.withColumn("cause", CAUSE).filter(F.col("cause").isNotNull())
          .groupBy("cause").agg(F.count("*").alias("lignes"), F.round(F.sum(contribution), 2).alias("impact_eur"))
          .orderBy("cause"))
causes_pd = causes.toPandas()
somme_causes = causes_pd["impact_eur"].sum()
arrondi = round(ecart - somme_causes, 2)          # le fichier Flink est arrondi au centime par minute

print(f"CA Flink (somme des 120 minutes)  : {ca_flink:>14,.2f} €")
print(f"CA Spark (couche gold)            : {ca_spark:>14,.2f} €")
print(f"Écart Flink - Spark               : {ecart:>+14,.2f} €  ({100 * ecart / ca_spark:+.2f} %)\n")
for _, r in causes_pd.iterrows():
    print(f"  {r['cause']:<42} {int(r['lignes']):>5} lignes  {r['impact_eur']:>+12,.2f} €"
          f"  ({100 * r['impact_eur'] / ca_spark:+.2f} %)")
print(f"  {'arrondis au centime du fichier Flink':<42} {'':>5}         {arrondi:>+12,.2f} €")
print(f"  {'TOTAL des causes':<42} {'':>5}         {somme_causes + arrondi:>+12,.2f} €")
assert abs(somme_causes + arrondi - ecart) < 0.01

# %% [markdown]
# Graphique en cascade de l'écart, et comparaison minute par minute :

# %%
def euros(v, signe=False):
    return (f"{v:+,.0f}" if signe else f"{v:,.0f}").replace(",", " ") + " €"


impacts = list(causes_pd["impact_eur"])
libelles = ["CA Spark\n(couche gold)", "+ doublons\ncomptés par Flink", "+ pays absent\ncompté par Flink",
            "− événements tardifs\nignorés par Flink", "CA Flink\n(temps réel)"]
niveaux = [ca_spark]
for v in impacts:
    niveaux.append(niveaux[-1] + v)
bas_axe = min(niveaux + [ca_flink]) - 0.25 * (max(niveaux) - min(niveaux))
haut_axe = max(niveaux) + 0.12 * (max(niveaux) - min(niveaux))
fig, ax = plt.subplots(figsize=(9, 4.4))
ax.bar(0, ca_spark - bas_axe, bottom=bas_axe, color="#4c78a8", width=0.6)
ax.text(0, ca_spark, euros(ca_spark), ha="center", va="bottom", fontsize=9, fontweight="bold")
for i, v in enumerate(impacts, start=1):
    ax.bar(i, abs(v), bottom=min(niveaux[i - 1], niveaux[i]), width=0.6,
           color="#59a14f" if v >= 0 else "#d1495b")
    ax.plot([i - 1.3, i + 0.3], [niveaux[i - 1]] * 2, color="#999", lw=0.8, ls=":")
    ax.text(i, max(niveaux[i - 1], niveaux[i]), euros(v, signe=True), ha="center", va="bottom", fontsize=9)
fin = len(impacts) + 1
ax.bar(fin, ca_flink - bas_axe, bottom=bas_axe, color="#f28e2b", width=0.6)
ax.text(fin, ca_flink, euros(ca_flink), ha="center", va="bottom", fontsize=9, fontweight="bold")
ax.set_xticks(range(fin + 1))
ax.set_xticklabels(libelles, fontsize=8.5)
ax.set_ylim(bas_axe, haut_axe)
ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(lambda v, _: f"{v / 1e6:.2f} M€"))
ax.spines[["top", "right"]].set_visible(False)
ax.set_title(f"Écart Flink − Spark = {euros(ecart, signe=True)} ({100 * ecart / ca_spark:+.2f} %) : "
             "décomposition", fontsize=11)
fig.tight_layout()
fig.savefig(f"{FIG_DIR}/E2_cascade_ecart.png", dpi=150)

spark_minute = (propres_pq.groupBy(F.date_trunc("minute", "ts_evenement").alias("minute"))
                .agg(F.sum("montant").alias("ca_spark")))
comp = (flink.join(spark_minute, flink.w_start == spark_minute.minute)
        .select("w_start", F.col("total").alias("ca_flink"), "ca_spark").orderBy("w_start").toPandas())
fig, ax = plt.subplots(figsize=(9, 3.2))
ax.plot(comp["w_start"], comp["ca_flink"] - comp["ca_spark"], color="#d1495b", lw=1)
ax.axhline(0, color="#333", lw=0.8)
ax.set_title("Écart Flink - Spark par minute (€)")
fig.autofmt_xdate()
fig.tight_layout()
fig.savefig(f"{FIG_DIR}/E2_ecart_par_minute.png", dpi=150)
print(comp.assign(ecart=comp.ca_flink - comp.ca_spark).describe().round(2).loc[["mean", "min", "max"]])

# %% [markdown]
# **Pour la recommandation** : de combien les événements ignorés arrivent-ils après la fin de leur minute ?
# Combien de CA récupèrerait-on avec un watermark plus large ?

# %%
tardifs = lignes_e2.filter(~F.col("compte_flink") & F.col("compte_spark"))
tardifs.select(F.expr("percentile_approx(retard_s, array(0.5, 0.9, 0.99))").alias("retard_s_p50_p90_p99"),
               F.max("retard_s").alias("retard_max_s")).show(truncate=False)
perdu = tardifs.agg(F.sum("montant")).first()[0]
for wm in (5, 30, 60, 120):
    recupere = tardifs.filter(F.col("retard_s") <= wm).agg(F.sum("montant")).first()[0] or 0
    print(f"watermark {wm:>3} s : CA tardif encore perdu = {perdu - recupere:>10,.2f} €"
          f"  ({100 * (perdu - recupere) / ca_spark:.2f} % du CA)")

# %% [markdown]
# ## E3. Restitution — option lake : trois requêtes Spark SQL sur la couche gold

# %%
for nom in ("ca_par_pays", "ca_par_famille", "indicateurs", "rejets_par_motif"):
    spark.read.parquet(f"{GOLD_DIR}/{nom}").createOrReplaceTempView(f"gold_{nom}")

# Requête 1 : top 3 des pays et leur part du CA
spark.sql("""
    SELECT pays, ca, nb_commandes, panier_moyen, part_ca_pct
    FROM gold_ca_par_pays
    ORDER BY ca DESC
    LIMIT 3
""").show()

# Requête 2 : familles qui pèsent plus de 10 % du CA, avec le CA moyen par article vendu
spark.sql("""
    SELECT famille, ca, part_ca_pct, ROUND(ca / quantite_totale, 2) AS ca_par_article
    FROM gold_ca_par_famille
    WHERE part_ca_pct > 10
    ORDER BY ca DESC
""").show()

# Requête 3 : tableau de bord qualité du jour (taux de rejet, doublons, motif principal)
spark.sql("""
    SELECT i.date_donnees, i.nb_commandes_propres, i.taux_rejet_pct,
           ROUND(100 * i.nb_doublons / i.nb_lignes_brutes, 2) AS taux_doublons_pct,
           m.motif_rejet AS motif_principal, m.part_des_rejets_pct
    FROM gold_indicateurs i
    CROSS JOIN (SELECT * FROM gold_rejets_par_motif ORDER BY nb_rejets DESC LIMIT 1) m
""").show()

# %%
spark.stop()
