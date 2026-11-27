# Contenu inséré sous chaque question (index = bloc de l'énoncé)

# ---------------------------------------------------------------- préalable
Bloc(10).titre("Mon environnement et mes données").puces([
    "**Kafka et Flink** : le kit du cours sous Docker (3 brokers KRaft, Flink 1.20). **HDFS** : un conteneur Hadoop "
    "(1 NameNode + 1 DataNode), mêmes commandes `hdfs dfs` que sur Colab. **Spark** : PySpark en local, dans un notebook "
    "qui tourne aussi sur Colab.",
    "Je n'avais pas `Kit_Examen_Etudiants.zip` : j'ai généré un jeu de test **au même format** "
    "(`generer_donnees_exam1.py`, graine fixe) : 12 484 lignes sur 120 minutes, avec les anomalies de l'énoncé "
    "(doublons, pays et prix absents, quantités ≤ 0, dates invalides, retards, un produit majoritaire). "
    "`ca_minute_flink.csv` applique exactement les règles du job Flink données en E2. Avec les vrais fichiers, il suffit "
    "de relancer le notebook.",
])

# ---------------------------------------------------------------- A1
Bloc(13).titre().p("Conventions : 1 Ko = 1 000 octets, 1 Mo = 1 000 Ko, 1 jour = 86 400 s.").tableau(
    ["Grandeur", "Calcul", "Résultat"],
    [["Débit entrant", "20 000 msg/s × 1 Ko", "**20 Mo/s**"],
     ["Réplication entre brokers", "RF 3 : 2 copies en plus de celle du leader → 2 × 20", "**40 Mo/s**"],
     ["Sortant vers les consommateurs", "groupe de 10 = 1 lecture du flux + 2 applis × 1 lecture → 3 × 20", "**60 Mo/s**"],
     ["Sortant total des brokers", "60 (consommateurs) + 40 (réplication)", "**100 Mo/s**"],
     ["Stockage brut sur 3 jours", "20 Mo/s × 86 400 s × 3", "**5,18 To**"],
     ["Stockage avec réplication", "5,18 × 3", "**15,55 To**"],
     ["Capacité disque à prévoir", "15,55 / 0,7 (disques remplis à 70 % max)", "**≈ 22 To**"]],
    [4.6, 8.0, 3.0]
).p("**Partitions : 12.** Hypothèses : un consommateur traite ≈ 2 Mo/s, une partition encaisse ≈ 10 Mo/s. Dans un groupe, "
    "une partition n'est lue que par un seul consommateur : il en faut au moins 20 / 2 = 10 pour que les 10 consommateurs "
    "travaillent. Je prends 12 : 20 % de marge, et un multiple de 3 et de 4 pour répartir les leaders également."
).p("**Brokers : 4.** Hypothèse : 8 To par broker, remplis à 70 % = 5,6 To utiles. Stockage : 15,55 / 5,6 = 2,8 → 3 brokers. "
    "Réseau : 100 Mo/s / 3 = 33 Mo/s par broker, sous les 125 Mo/s d'une carte 1 Gb/s. J'ajoute 1 broker pour qu'une "
    "panne pendant une maintenance ne bloque pas les écritures."
).p("**Paramètres de durabilité :**").puces([
    "producteur : `acks=all` (attendre toutes les répliques à jour), `enable.idempotence=true` (un renvoi ne crée pas de "
    "doublon), `retries` élevé ;",
    "topic : `replication.factor=3`, `min.insync.replicas=2` (au moins 2 copies pour accepter une écriture → 1 panne "
    "tolérée sans perte), `unclean.leader.election.enable=false` (une réplique en retard ne devient jamais leader), "
    "`retention.ms=259200000` (3 jours).",
]).p("**Optimisation du stockage** : j'ai envoyé les mêmes 30 000 commandes avec 4 réglages producteur et mesuré la taille "
     "réelle sur le broker. Batching + zstd divise la taille par 6,5 : les 15,55 To tomberaient à ≈ 2,4 To."
).img("term_A3_optimisation_stockage.png")

# ---------------------------------------------------------------- A2
Bloc(15).titre().p(
    "Je démarre les 3 brokers (`docker compose up -d kafka1 kafka2 kafka3`), puis j'ouvre un shell dans un broker "
    "(`docker exec -it kafka1 bash`, `cd /opt/kafka/bin`) pour avoir les outils Kafka. Je crée le topic avec les 3 "
    "paramètres demandés, puis je le décris pour vérifier :"
).code("./kafka-topics.sh --bootstrap-server kafka1:9092 --create --topic exam_cmd \\",
       "    --partitions 6 --replication-factor 3 --config min.insync.replicas=3",
       "./kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic exam_cmd"
).img("term_A2_02_creation_topic.png").puces([
    "**Leader** : le broker qui reçoit lectures et écritures de la partition (3, 1, 2, 3, 1, 2 : 2 partitions chacun).",
    "**Replicas** : les brokers qui ont une copie (les 3, car RF 3) ; le premier est le leader préféré.",
    "**Isr** (*In-Sync Replicas*) : les répliques **à jour** avec le leader. Ici les 3 : tout est synchronisé. Seule une "
    "réplique de l'ISR peut devenir leader sans perte.",
])

Bloc(16).titre().p("J'arrête **un seul** broker (règle de sécurité) et je redécris le topic :").code(
    "docker compose stop kafka3"
).img("term_A2_03_panne_kafka3.png").puces([
    "`Isr` passe à **1,2** partout : kafka3 n'est plus à jour.",
    "Les partitions 0 et 3, dont kafka3 était leader, ont un nouveau leader (le broker 1), choisi dans l'ISR.",
    "`Replicas` ne change pas : c'est l'affectation prévue des copies, pas leur état.",
])

Bloc(17).titre().p("J'envoie un message avec le producteur console, en forçant `acks=all` :").code(
    'echo "commande-test-1" | ./kafka-console-producer.sh --bootstrap-server kafka1:9092 \\',
    "    --topic exam_cmd --producer-property acks=all"
).img("term_A2_04_ecriture_acks_all_echec.png").p(
    "Le leader répond `NOT_ENOUGH_REPLICAS` ; le producteur réessaie 3 fois puis abandonne avec "
    "`NotEnoughReplicasException`."
)

Bloc(18).titre().p(
    "Avec `acks=all`, le leader n'acquitte un message que lorsque **toutes les répliques de l'ISR** l'ont écrit. "
    "`min.insync.replicas=3` lui interdit d'écrire si l'ISR compte **moins de 3** répliques. kafka3 étant arrêté, l'ISR "
    "n'en a plus que 2 : le message est **refusé avant d'être écrit**. Il n'est donc pas perdu, et le producteur est "
    "prévenu par une exception. Avec `min.insync.replicas` égal au facteur de réplication, la moindre panne bloque "
    "toutes les écritures."
)

Bloc(19).titre().p(
    "Je passe `min.insync.replicas` à 2 (= RF − 1) : un message acquitté est alors sur au moins 2 brokers, donc la perte "
    "d'un broker ne perd rien, et l'ISR de 2 suffit pour écrire."
).code("./kafka-configs.sh --bootstrap-server kafka1:9092 --alter --entity-type topics \\",
       "    --entity-name exam_cmd --add-config min.insync.replicas=2"
).p("Preuve : j'écris 6 messages en `acks=all`, je les relis avec leur partition et leur offset, puis je contrôle les "
    "offsets de fin :"
).code('printf "commande-1\\n...\\ncommande-6\\n" | ./kafka-console-producer.sh \\',
       "    --bootstrap-server kafka1:9092 --topic exam_cmd --producer-property acks=all",
       "./kafka-console-consumer.sh --bootstrap-server kafka1:9092 --topic exam_cmd --from-beginning \\",
       "    --property print.partition=true --property print.offset=true",
       "./kafka-get-offsets.sh --bootstrap-server kafka1:9092 --topic exam_cmd"
).img("term_A2_05_correction_min_isr.png").puces([
    "Les 6 messages sont acquittés et relus. Ils sont tous en partition 4 : sans clé, le producteur remplit une "
    "partition à la fois.",
    "Le message refusé à l'étape 3 n'existe pas (l'offset 0 est `commande-1`) : il n'avait jamais été écrit.",
    "Le `describe` de la config montre que mon réglage du topic (2) l'emporte sur celui du broker et sur le défaut.",
    "Le `TimeoutException` final est normal : c'est l'arrêt du consommateur demandé par `--timeout-ms`.",
])

Bloc(20).titre().code(
    "docker compose start kafka3",
    "./kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic exam_cmd",
    "./kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic exam_cmd --under-replicated-partitions"
).img("term_A2_06_retour_kafka3.png").puces([
    "kafka3 récupère chez les leaders les messages écrits pendant son absence, puis revient dans l'ISR : "
    "**Isr: 1,2,3** partout, en 4 s.",
    "`--under-replicated-partitions` ne renvoie rien : plus aucune partition en défaut. Les offsets lus sur kafka3 sont "
    "identiques : sa copie est complète.",
    "Je lance une élection préférée (`kafka-leader-election.sh --election-type preferred`) pour lui rendre le rôle de "
    "leader des partitions 0 et 3.",
])

# ---------------------------------------------------------------- B
Bloc(23).titre("Mise en place").p(
    "La création automatique des topics est désactivée : je crée le topic source, je démarre Flink et j'ouvre le client "
    "SQL, qui charge la table `commandes` (watermark 5 s) avec `init.sql` :"
).code("./kafka-topics.sh --bootstrap-server kafka1:9092 --create --topic commandes --partitions 6 --replication-factor 3",
       "docker compose --profile flink up -d --build",
       "docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql"
).p("Je lance les jobs **avant** le générateur : la table lit en `latest-offset`, elle ne voit que ce qui est produit "
    "après son démarrage."
).code("python generator.py --rate 50 --duration 300 --late-prob 0.1 --max-delay 40")

Bloc(24).titre().p(
    "**Démarche.** Je déclare 2 tables de plus sur le **même** topic, identiques à `commandes` sauf le watermark (30 s et "
    "60 s), chacune avec son `group.id` pour lire tout le flux. Trois jobs identiques calculent le CA et le nombre de "
    "commandes par minute et écrivent dans `ca_wm5`, `ca_wm30`, `ca_wm60` (fichier `B1_watermarks.sql`) :"
).code("CREATE TABLE commandes_wm30 ( ..., ts TIMESTAMP(3),",
       "  WATERMARK FOR ts AS ts - INTERVAL '30' SECOND ) WITH ('topic' = 'commandes', ...);",
       "INSERT INTO ca_wm30",
       "SELECT 'wm30', window_start, window_end, COUNT(*), ROUND(SUM(amount), 2), CURRENT_TIMESTAMP",
       "FROM TABLE(TUMBLE(TABLE commandes_wm30, DESCRIPTOR(ts), INTERVAL '1' MINUTE))",
       "WHERE amount > 0 GROUP BY window_start, window_end;"
).img("B1_flink_3_jobs.png").p(
    "**Mesure.** Les topics de sortie sont créés en `message.timestamp.type=LogAppendTime` : Kafka horodate chaque "
    "résultat à sa réception, donc délai = horodatage − fin de la minute. Pour le nombre réel de commandes, mon script "
    "`B1_mesure.py` relit tout le topic `commandes` minute par minute."
).img("term_B1_02_tableau_watermarks.png").tableau(
    ["Minute 20:52 (complète)", "Commandes comptées", "Perdues", "Délai d'émission"],
    [["Réel (topic relu)", "2 897", "–", "–"],
     ["Watermark 5 s", "2 834", "−2,2 %", "5,6 s"],
     ["Watermark 30 s", "2 891", "−0,2 %", "30,4 s"],
     ["Watermark 60 s", "2 897", "0 %", "60,7 s"]],
    [5.0, 3.6, 2.8, 4.2]
).p("**Explication.** Une fenêtre est émise quand le watermark (plus grand `ts` vu − délai) dépasse sa fin : le délai "
    "d'émission vaut donc le watermark. Ce qui arrive ensuite pour cette minute est ignoré. La perte se calcule : 10 % "
    "des événements sont retardés de 1 à 40 s, et un événement est perdu si sa seconde dans la minute + son retard "
    "> 60 + délai. Cela donne 2,6 % (5 s), 0,2 % (30 s) et 0 % (60 s), ce que je mesure. Les minutes 20:49 et 20:50 sont "
    "incomplètes (démarrage du générateur)."
)

Bloc(25).titre().puces([
    "**(a) Alerte de fraude : 5 s.** Une alerte doit être immédiate (5,6 s au lieu de 60 s). Rater environ 2,6 % "
    "d'événements très en retard est acceptable : le batch les rattrape.",
    "**(b) Rapport de fin de journée : au moins le retard maximal observé (40 s, donc 60 s)**, pour 0 % de perte ; la "
    "latence n'a pas d'importance. En pratique, ce rapport vient plutôt du batch Spark, qui voit tous les événements.",
])

Bloc(26).titre().p(
    "Je crée le topic, puis j'adapte `05_alertes.sql` : topic `alertes_exam`, regroupement **par client et par minute**, "
    "et seule la règle du nombre de commandes (fichier `B3_alertes_exam.sql`). Le `HAVING` filtre après le comptage :"
).code("./kafka-topics.sh --bootstrap-server kafka1:9092 --create --topic alertes_exam --partitions 3 --replication-factor 3",
       "INSERT INTO alertes_exam",
       "SELECT customer_id, window_start, window_end, COUNT(*) AS nb, ROUND(SUM(amount), 2) AS total,",
       "       'plus de 3 commandes en 1 minute' AS motif",
       "FROM TABLE(TUMBLE(TABLE commandes, DESCRIPTOR(ts), INTERVAL '1' MINUTE))",
       "WHERE amount > 0",
       "GROUP BY customer_id, window_start, window_end",
       "HAVING COUNT(*) > 3;"
).p("Je lis quelques alertes avec un consommateur console :").code(
    "./kafka-console-consumer.sh --bootstrap-server kafka1:9092 --topic alertes_exam --from-beginning --max-messages 8"
).img("term_B3_02_alertes_consommateur.png").p(
    "Environ 330 alertes par minute, ce qui est attendu : 50 commandes/s sur 1 000 clients, soit 2,9 commandes par client "
    "et par minute. Avec une loi de Poisson, P(X > 3) = 33 %, donc 333 alertes attendues. Le seuil est trop bas pour ce "
    "trafic."
)

Bloc(27).titre().p("**Panne.** Pendant que le job B3 tourne, j'arrête puis je redémarre le TaskManager :").code(
    "docker compose stop taskmanager", "docker compose start taskmanager"
).img("B4_exceptions.png").puces([
    "Sans TaskManager, il n'y a plus de slot pour exécuter les tâches : elles passent en `FAILED`, le job en "
    "**RESTARTING**, et Flink réessaie en boucle (`NoResourceAvailableException`).",
    "Au redémarrage, le job repart **du dernier checkpoint** (pris toutes les 10 s) : il restaure les offsets Kafka et "
    "l'état des fenêtres, puis relit ce qui a suivi.",
]).img("B4_checkpoints_restauration.png").p(
    "**Doublons : oui**, avec le puits par défaut (at-least-once). Les alertes écrites entre le dernier checkpoint et la "
    "panne sont recalculées après la reprise, puis réécrites."
).p("**Vérification.** Une alerte est identifiée par `(customer_id, w_start)`. Mon script `B4_doublons.py` relit tout le "
    "topic et compte les clés vues plusieurs fois. Au 1er essai : 0 doublon, car le checkpoint était passé juste après "
    "l'émission. Au 2e essai, avec la panne entre l'émission et le checkpoint : **338 doublons**."
).img("term_B4_04_verification_doublons_2e_essai.png").p(
    "**Garantie à configurer : exactly-once.** Le puits écrit dans une **transaction Kafka**, validée seulement à la fin "
    "du checkpoint, et les lecteurs lisent en `read_committed` (fichier `B4_alertes_exactly_once.sql`) :"
).code("'sink.delivery-guarantee' = 'exactly-once',",
       "'sink.transactional-id-prefix' = 'alertes-exam-eos'"
).img("term_B4_11_transaction_annulee_visible_en_read_uncommitted.png").p(
    "Avec la même panne en exactly-once, la transaction interrompue reste dans le log (654 alertes en `read_uncommitted`) "
    "mais elle est annulée : un lecteur `read_committed` voit 327 alertes, soit **0 doublon**."
).p("**Coût** : les résultats ne sont visibles qu'au checkpoint suivant (9,1 s au lieu de 5,5 s, jusqu'à +10 s), Kafka a "
    "plus de travail (transactions), et tous les lecteurs doivent passer en `read_committed`."
).p("Un checkpoint de ce job contient (vérifié sur disque) : les offsets Kafka et le watermark des sources, les "
    "accumulateurs des fenêtres ouvertes de chaque client avec leurs timers, et les transactions à valider. Pas les "
    "commandes : c'est Kafka qui les rejoue."
)

# ---------------------------------------------------------------- C
Bloc(30).titre().puces([
    "Fichiers : 100 × 60 × 24 × 182 = **26 208 000**.",
    "Mémoire : chaque fichier fait moins de 128 Mo, donc 1 bloc, soit 2 objets. 26 208 000 × 2 × 150 octets = "
    "**7,9 Go** de RAM NameNode.",
    "**Conclusion** : intenable. Le NameNode garde toute la carte des fichiers en RAM (JVM énorme, redémarrage lent, "
    "sollicité à chaque accès), et Spark lancerait une tâche par fichier.",
    "**Stratégie ÷ 60** : un fichier par entrepôt **et par heure**, soit 100 × 24 × 182 = **436 800 fichiers** (÷ 60) et "
    "≈ 131 Mo de RAM. Encore mieux : des fichiers de 128 Mo à 1 Go, partitionnés par date et heure.",
])

Bloc(32).titre().p("Je crée toute l'arborescence en une commande ; `-p` crée aussi les répertoires parents :").code(
    "hdfs dfs -mkdir -p /data/raw/commandes/ingestion_date=2026-11-02 /data/raw/produits \\",
    "    /data/curated /data/analytics /data/rejects"
).p("Rôle des zones : `raw` garde le brut intact pour pouvoir tout rejouer, `curated` les données nettoyées, `rejects` "
    "les lignes écartées avec leur motif, `analytics` la couche gold. `ingestion_date=` est un partitionnement « Hive » : "
    "Spark ne lira que la date demandée."
)

Bloc(33).titre().code(
    "hdfs dfs -put commandes_exam1.csv /data/raw/commandes/ingestion_date=2026-11-02/",
    "hdfs dfs -put produits_exam1.csv /data/raw/produits/",
    "hdfs dfs -ls -R /data"
).img("term_C2_01_arborescence.png")

Bloc(34).titre().code(
    "hdfs fsck /data/raw/commandes/ingestion_date=2026-11-02/commandes_exam1.csv -files -blocks -locations"
).img("term_C2_02_fsck.png", hauteur_max=12).puces([
    "**1 bloc** : le fichier fait 1 Mo, bien moins qu'un bloc de 128 Mo.",
    "**Réplication effective 1** (`Live_repl=1`) : il n'y a qu'un DataNode, donc `dfs.replication=1`, comme sur Colab. "
    "En production, ce serait 3.",
    "`HEALTHY` : aucun bloc sous-répliqué, manquant ou corrompu.",
])

Bloc(35).titre().code(
    "split -l 600 -d -a 2 --additional-suffix=.csv commandes_exam1.csv split/commandes_part_",
    "hdfs dfs -mkdir -p /data/raw/commandes_split && hdfs dfs -put split/*.csv /data/raw/commandes_split/",
    "hdfs dfs -count -v /data/raw/commandes_split",
    "hdfs dfs -cat /data/raw/commandes_split/*.csv | hdfs dfs -put - /data/raw/commandes_compacte/commandes_exam1.csv"
).img("term_C2_03_split_count_compaction.png").tableau(
    ["", "Fichiers", "Blocs", "Objets NameNode", "Taille"],
    [["Découpé (600 lignes)", "21", "21", "42", "1 011 127 o"],
     ["Compacté", "1", "1", "2", "1 011 127 o"]],
    [4.4, 2.4, 2.4, 3.4, 3.0]
).p("`-count` affiche répertoires, fichiers et octets. Le contenu est identique (même md5), mais il coûte 21 fois plus "
    "d'objets au NameNode : c'est le problème de C1 en miniature. Je compacte par concaténation (`-cat` puis `-put -`)."
)

Bloc(36).titre().puces([
    "Écrire avec un connecteur qui **accumule** (Kafka Connect HDFS Sink, ou sink fichier Flink), jamais message par "
    "message.",
    "Fermer les fichiers **à la taille** (128 Mo à 1 Go) **ou toutes les heures**, au premier atteint.",
    "Partitionner par **date et heure**, pas par entrepôt (l'entrepôt reste une colonne).",
    "Écrire en **Parquet compressé**.",
    "**Compacter** chaque nuit la partition de la veille ; Kafka (3 jours de rétention) sert de tampon.",
])

# ---------------------------------------------------------------- D
Bloc(39).titre("Chargement").p(
    "Je lis toutes les colonnes **en texte** avec un schéma imposé, puis je convertis moi-même. Si Spark devinait les "
    "types, une date ou une quantité illisible deviendrait NULL sans que je puisse la compter."
).code('brut = spark.read.csv("commandes_exam1.csv", header=True,',
       '    schema="id_commande STRING, id_entrepot STRING, id_produit STRING, pays STRING, quantite STRING, "',
       '           "prix_unitaire STRING, ts_evenement STRING, ts_ingestion STRING")',
       'cmd = (brut.withColumn("quantite_i", F.col("quantite").cast("int"))',
       '           .withColumn("prix", F.col("prix_unitaire").cast("double"))',
       '           .withColumn("ts_evt", F.to_timestamp("ts_evenement", "yyyy-MM-dd HH:mm:ss.SSS")))'
)

Bloc(40).titre().p("Un seul `agg`, donc une seule lecture des données : chaque condition devient 0 ou 1 et `sum` compte "
                   "les 1.").code(
    "qualite = cmd.agg(",
    '    F.count("*").alias("nb_lignes"),',
    '    (F.count("*") - F.countDistinct("id_commande")).alias("doublons_id_commande"),',
    '    *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in COLONNES],',
    '    F.sum((F.col("quantite_i") <= 0).cast("int")).alias("quantite_<=0"),',
    '    F.sum((F.col("ts_evenement").isNotNull() & F.col("ts_evt").isNull()).cast("int")).alias("dates_invalides"))'
).tableau(
    ["Contrôle", "Valeur", "% des lignes"],
    [["Lignes", "12 484", "100 %"],
     ["Doublons sur id_commande (lignes − id distincts)", "**377**", "3,02 %"],
     ["NULL id_commande, id_entrepot, id_produit, quantite, ts_ingestion", "0", "0 %"],
     ["NULL pays", "**190**", "1,52 %"],
     ["NULL prix_unitaire", "**149**", "1,19 %"],
     ["NULL ts_evenement (date vide)", "12", "0,10 %"],
     ["Quantités ≤ 0", "**249**", "1,99 %"],
     ["Dates invalides (renseignées mais illisibles)", "**88**", "0,70 %"]],
    [10.0, 2.6, 3.0]
).p("Exemples de dates invalides : `2026-02-30`, `25:13:07`, `02/11/2026 08:15`, `N/A`.")

Bloc(41).titre().puces([
    "**Dédoublonnage** : je garde la **première arrivée** de chaque `id_commande` avec `row_number()` trié par "
    "`ts_ingestion`. `dropDuplicates` garderait une copie au hasard.",
    "**motif_rejet** : un seul motif par ligne, dans l'ordre de l'énoncé (pays, quantité, prix, date).",
    "**Écriture** dans deux dossiers Parquet distincts, `coalesce(1)` pour ne pas créer de petits fichiers.",
]).code('w = Window.partitionBy("id_commande").orderBy("ts_ing")',
        'lignes = cmd.withColumn("rang", F.row_number().over(w)).withColumn("motif_rejet", MOTIF)',
        'propres.coalesce(1).write.parquet("lake/curated/commandes")',
        'rejets.coalesce(1).write.parquet("lake/rejects/commandes")'
).p("**Preuve** : je relis les deux dossiers **écrits** et je compare au nombre d'`id_commande` distincts du fichier brut :"
).code("propres (11449) + rejets (658) = 12107",
       "commandes distinctes                 = 12107   -> OK"
).tableau(
    ["Motif", "Rejets"],
    [["quantite_invalide", "236"], ["pays_absent", "186"], ["prix_absent", "141"], ["date_invalide", "95"],
     ["**Total**", "**658**"]],
    [6.0, 3.0]
).p("Ces chiffres sont plus bas qu'en D1 : les copies sont retirées avant de chercher le motif, et une ligne n'a qu'un "
    "motif.")

Bloc(42).titre().p("Je désactive broadcast et AQE pour voir la jointure brute, puis j'affiche le plan :").code(
    'spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")',
    'spark.conf.set("spark.sql.adaptive.enabled", "false")',
    'ca_famille = propres.join(produits, "id_produit").groupBy("famille").agg(F.sum("montant"))',
    "ca_famille.explain()"
).p("**SortMergeJoin et 4 Exchange** (shuffles) : 2 pour la jointure (les deux tables sont redistribuées par `id_produit`), "
    "1 pour le `groupBy`, 1 pour le tri final."
).code("SortMergeJoin [id_produit], [id_produit], Inner",
       ":- Exchange hashpartitioning(id_produit, 200)     <- commandes redistribuées",
       "+- Exchange hashpartitioning(id_produit, 200)     <- produits redistribués"
).p("Répartition : P007 = 4 611 lignes, soit **40,3 %** (286 en moyenne par produit). Après `repartition('id_produit')`, "
    "toutes les lignes d'un produit tombent dans la même partition :"
).code('propres.repartition("id_produit").groupBy(F.spark_partition_id()).count()'
).img("D3_skew_partitions.png").p(
    "38 partitions non vides sur 200 ; la plus grosse (P007) fait **25,8 fois la médiane**. **Skew fort** : une tâche "
    "fait 40 % du travail, et une étape dure autant que sa tâche la plus lente."
)

Bloc(43).titre().p(
    "Je choisis le **broadcast join** : la table produits (40 lignes, 1,4 Ko) est envoyée à chaque tâche. Les commandes ne "
    "sont plus redistribuées par produit, donc il n'y a plus de partition P007."
).code('gros.join(F.broadcast(produits), "id_produit").groupBy("famille").agg(F.sum("montant"))'
).p("Mesuré sur une copie ×100 (1,14 million de lignes), avec les métriques par tâche de l'API REST de Spark :").tableau(
    ["", "Avant (SortMergeJoin)", "Après (broadcast)"],
    [["Exchange (shuffle)", "3", "1"],
     ["Lignes envoyées en shuffle", "1 144 980", "20"],
     ["Tâche la plus chargée", "461 101 lignes", "286 226 lignes"],
     ["Déséquilibre max / médiane", "**25,8**", "**1,0**"],
     ["Durée moyenne", "1,45 s", "**0,73 s**"]],
    [5.6, 5.0, 5.0]
).p("**Pourquoi c'est le plus adapté ici** : la table de dimension est minuscule, donc le shuffle disparaît. Le salting "
    "dupliquerait N fois les produits (il sert quand les deux tables sont grosses), et l'AQE ne découpe que les "
    "partitions de plus de 256 Mo. D'ailleurs, avec les réglages par défaut, Spark choisit seul le broadcast :"
).img("term_D4_reglages_par_defaut_spark.png")

# ---------------------------------------------------------------- E
Bloc(46).titre().p("J'écris 4 tables Parquet, un fichier chacune, dans `gold/` puis dans `/data/analytics` :").code(
    'gold_ca_pays = propres.groupBy("pays").agg(F.round(F.sum("montant"), 2).alias("ca"),',
    '                                           F.count("*").alias("nb_commandes"))',
    'gold_ca_pays.coalesce(1).write.mode("overwrite").parquet("gold/ca_par_pays")'
).tableau(
    ["Pays", "CA (€)", "Part"],
    [["FR", "1 491 084,30", "38,6 %"], ["DE", "581 279,58", "15,0 %"], ["BE", "459 768,94", "11,9 %"],
     ["ES", "431 090,63", "11,2 %"], ["IT", "349 669,42", "9,0 %"], ["CH", "287 178,09", "7,4 %"],
     ["MA", "133 240,54", "3,4 %"], ["TN", "132 201,83", "3,4 %"]],
    [3.0, 4.0, 3.0]
).tableau(
    ["Famille", "CA (€)", "Part"],
    [["Informatique", "1 880 673,93", "48,7 %"], ["Réseau", "748 150,97", "19,4 %"],
     ["Mobilier", "640 866,96", "16,6 %"], ["Outillage", "477 369,57", "12,3 %"], ["Consommables", "118 451,90", "3,1 %"]],
    [4.0, 4.0, 3.0]
).p("**11 449 commandes propres** ; taux de rejet = 658 / 12 107 = **5,43 %** (je divise par les commandes distinctes : "
    "un doublon n'est pas une commande). CA total **3 865 513,33 €**."
)

Bloc(48).titre().p(
    "**Méthode.** Je rejoue ligne par ligne sur le brut, doublons compris, les règles Flink : quantité > 0, prix "
    "renseigné, date lisible, `ts_ingestion` ≤ fin de minute + 5 s. Puis les règles Spark : première arrivée et aucun "
    "motif de rejet. Chaque ligne apporte montant × (compté Flink − compté Spark) à l'écart. **Contrôle** : mes règles "
    "Flink redonnent exactement `ca_minute_flink.csv` (0 écart sur les 120 minutes)."
).tableau(
    ["", "Lignes", "Montant", "% du CA Spark"],
    [["CA Flink", "", "3 709 347,68 €", ""], ["CA Spark", "", "3 865 513,33 €", ""],
     ["**Écart Flink − Spark**", "", "**−156 165,65 €**", "**−4,04 %**"],
     ["1. doublons comptés par Flink", "295", "+96 432,79 €", "+2,49 %"],
     ["2. pays absent compté par Flink", "166", "+54 915,40 €", "+1,42 %"],
     ["3. événements tardifs ignorés par Flink", "824", "−307 513,84 €", "−7,96 %"],
     ["**Somme des causes**", "", "**−156 165,65 €**", "**−4,04 %**"]],
    [6.2, 1.8, 4.0, 3.6]
).img("E2_cascade_ecart.png").p(
    "295 copies sur 377 : les 82 autres sont tardives ou invalides, donc ignorées aussi par Flink. Les 824 tardifs sont "
    "arrivés plus de 5 s après leur minute (retard médian 46 s, max 116 s). L'écart net de −4 % cache des écarts bruts de "
    "+3,9 % et −8 % qui se compensent."
).p("**Recommandation** :").puces([
    "retards : garder 5 s pour l'écran, mais corriger les minutes quand les retardataires arrivent (sortie upsert), ou "
    "passer à 120 s là où la complétude compte (0 % perdu) ;",
    "dédoublonner dans Flink sur `id_commande` (`ROW_NUMBER()` + TTL d'état) ;",
    "appliquer les mêmes règles qualité dans les deux chemins ;",
    "afficher Flink comme provisoire et réconcilier chaque jour.",
])

Bloc(51).titre().p("Je choisis l'**option lake**. Mes trois requêtes Spark SQL sur la couche gold :").code(
    "-- 1. Top 3 des pays",
    "SELECT pays, ca, part_ca_pct FROM gold_ca_par_pays ORDER BY ca DESC LIMIT 3;",
    "-- 2. Familles au-dessus de 10 % du CA, avec le CA par article vendu",
    "SELECT famille, ca, ROUND(ca / quantite_totale, 2) AS ca_par_article",
    "FROM gold_ca_par_famille WHERE part_ca_pct > 10;",
    "-- 3. Qualité du jour : taux de rejet, taux de doublons, motif principal",
    "SELECT i.taux_rejet_pct, ROUND(100 * i.nb_doublons / i.nb_lignes_brutes, 2), m.motif_rejet",
    "FROM gold_indicateurs i CROSS JOIN",
    "  (SELECT * FROM gold_rejets_par_motif ORDER BY nb_rejets DESC LIMIT 1) m;"
).p("Résultats : FR 38,6 %, DE 15,0 %, BE 11,9 % ; Informatique, Réseau, Mobilier et Outillage dépassent 10 % ; rejet "
    "5,43 %, doublons 3,02 %, motif principal `quantite_invalide`."
).tableau(
    ["Colonne", "Définition", "Règle de calcul"],
    [["pays", "pays de la commande", "valeur source, non nulle (sinon rejet)"],
     ["famille", "famille du produit", "jointure avec produits_exam1 sur id_produit"],
     ["ca", "chiffre d'affaires (€)", "Σ quantite × prix_unitaire des commandes propres"],
     ["nb_commandes", "commandes propres", "COUNT(*) après dédoublonnage et rejets"],
     ["panier_moyen", "CA moyen par commande", "ca / nb_commandes"],
     ["part_ca_pct", "poids dans le CA", "100 × ca / CA total"],
     ["nb_doublons", "lignes en double", "lignes brutes − id_commande distincts"],
     ["taux_rejet_pct", "part des commandes rejetées", "100 × rejets / commandes distinctes"]],
    [3.4, 5.0, 7.2]
)

Bloc(52).titre().tableau(
    ["Élément", "Définition"],
    [["Formule", "CA = Σ (quantite × prix_unitaire) en euros (hypothèse : prix HT, hors remises et retours)"],
     ["Inclusion", "commandes distinctes (première arrivée) avec pays, quantité > 0, prix et date valides ; rattachées "
                   "à la date de l'**événement** ; retards inclus"],
     ["Exclusion", "doublons ; lignes rejetées (gardées dans /data/rejects avec leur motif) ; annulations et retours"],
     ["Source de vérité", "**Spark (couche gold)** : complet, dédoublonné, rejouable. Flink = CA temps réel **provisoire**"],
     ["Fraîcheur", "temps réel < 70 s (minute + watermark 5 s) ; officiel J+1 avant 8 h"],
     ["Contrôle", "réconciliation Flink / Spark quotidienne, alerte si l'écart dépasse 1 %"]],
    [3.4, 12.2]
)

# ---------------------------------------------------------------- F
Bloc(54).titre().img("F_architecture.png", largeur=15.6).p(
    "Architecture **Lambda** : le même flux Kafka alimente un chemin **temps réel** (Flink, rapide mais provisoire) et un "
    "chemin **consolidé** (dépôt horaire dans HDFS, puis Spark, complet et officiel). Le contrôle d'écart (en violet) "
    "industrialise ma réconciliation E2."
)

Bloc(55).titre().tableau(
    ["Risque", "Parade"],
    [["**Panne d'un broker** : écritures bloquées ou données perdues",
      "RF 3, min.insync.replicas 2, acks=all, idempotence, élection « unclean » désactivée, alerte sur les partitions "
      "sous-répliquées (vu en A2)"],
     ["**Doublons et chiffres qui divergent** entre temps réel et consolidé",
      "Flink exactly-once + lecteurs read_committed, dédoublonnage dans les deux chemins, réconciliation quotidienne "
      "(vu en B4 et E2)"],
     ["**Petits fichiers HDFS** : NameNode saturé",
      "dépôt par taille et par heure, partitions par date, compaction nocturne (vu en C)"]],
    [6.0, 9.6]
)

Bloc(56).titre().p(
    "**Un seul nettoyage partagé.** Un job Flink « qualité » dédoublonne sur `id_commande`, applique les règles de rejet, "
    "et écrit en exactly-once un topic `commandes_propres`, lu à la fois par le temps réel et par le dépôt HDFS. Les écarts "
    "dus aux doublons et au pays (+3,9 %) disparaissent par construction. Pour les retards, les agrégats vont dans une "
    "table qui accepte les mises à jour (Iceberg, Delta ou Paimon) : chaque minute est corrigée à l'arrivée de ses "
    "retardataires, et Spark comme Power BI lisent la même table."
)
