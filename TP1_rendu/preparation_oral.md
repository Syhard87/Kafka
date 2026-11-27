---
title: "Préparation à l'oral"
subtitle: "TP 1 : de l'ingestion au KPI. Comprendre, justifier, répondre"
lang: fr
---

# Comment utiliser ce dossier

* Chaque partie commence par **les mécanismes** (comment ça marche à l'intérieur), puis donne **les questions que la
  prof peut poser**, avec une réponse justifiée.
* Pour s'entraîner : lire la question en gras, **répondre à voix haute**, puis comparer avec la réponse.
* Tout ce qui est affirmé ici a été **vérifié sur le cluster** du TP : fichiers sur disque, contenu d'un checkpoint,
  mesures. Les preuves sont dans `sorties/` et `captures/`, et on peut les citer.
* La méthode pour toute question : **le mécanisme**, puis **le chiffre**, puis **le compromis**. Exemple : « l'ISR est
  l'ensemble des répliques à jour ; on l'a vu passer de 3 à 2 quand kafka3 s'est arrêté ; c'est pour ça qu'il faut
  `min.insync.replicas=2` et pas 3 ».

# 1. La vision : pourquoi cette chaîne existe

## Le problème métier

La direction voit deux chiffres d'affaires différents pour la même journée : celui de l'écran temps réel et celui du
rapport consolidé. Le risque n'est pas technique, il porte sur la **confiance** : si personne ne sait lequel est juste, on
ne pilote plus avec aucun des deux. Le TP demande de construire la chaîne, puis de **prouver d'où vient l'écart**.

## Pourquoi deux chemins (architecture Lambda)

On ne peut pas avoir à la fois un résultat **immédiat** et un résultat **complet** : pour être complet, il faut attendre
les retardataires, et attendre, c'est ne plus être immédiat.

| | Chemin temps réel (Kafka → Flink) | Chemin consolidé (Kafka → HDFS → Spark) |
|---|---|---|
| Question | « que se passe-t-il maintenant ? » | « combien a-t-on vraiment vendu ? » |
| Latence | secondes | heures (J+1) |
| Complétude | partielle : ignore ce qui arrive après le watermark | totale : tout est arrivé quand on calcule |
| Utilisateurs | logistique, fraude, pilotage | finance, direction, reporting |
| Nature du chiffre | **provisoire** | **officiel** (source de vérité) |

Cette organisation s'appelle l'**architecture Lambda**. Son défaut : deux codes, donc deux définitions qui divergent, et
c'est exactement l'écart du TP. L'alternative est l'**architecture Kappa** : un seul chemin en flux, rejouable grâce à la
rétention Kafka. C'est vers elle que va l'amélioration proposée en F.

## Le rôle de chaque brique, en une phrase

| Brique | Rôle | Pourquoi pas autre chose |
|---|---|---|
| **Kafka** | journal **durable, ordonné et rejouable** qui découple ceux qui écrivent de ceux qui lisent | une base de données ne supporte pas 20 000 écritures/s relues par 3 applications ; une file classique efface le message une fois lu, donc on ne peut pas rejouer |
| **Flink** | calcul **continu, avec état**, en temps d'événement | Spark Streaming fonctionne par micro-batchs et gère moins finement le temps d'événement ; Flink traite événement par événement |
| **HDFS** | stockage distribué **bon marché** pour de gros fichiers immuables | Kafka n'est pas fait pour garder 6 mois d'historique ; une base relationnelle coûte trop cher à ce volume |
| **Spark** | calcul **batch massif** : qualité, jointures, agrégats | un seul serveur ne peut pas traiter des téraoctets |
| **Couche gold + BI** | indicateurs **petits, précalculés et définis une fois** | recalculer 6 mois de données à chaque clic serait lent et donnerait des définitions différentes |

## Les trois garanties qu'on veut savoir nommer

1. **Durabilité** : un message acquitté n'est jamais perdu (partie A).
2. **Cohérence** : un événement compte **une fois** et une seule (parties B et D : doublons).
3. **Exactitude** : on calcule **la bonne chose**, avec une définition écrite (partie E).

# 2. Kafka en profondeur

## 2.1 Comment Kafka stocke un topic (vérifié sur le disque du broker)

Chaque **partition** est un répertoire sur le disque du broker. Pour `commandes-0` :

```
00000000000000000000.log          2,3 Mo   les messages, ajoutés à la fin, jamais modifiés
00000000000000000000.index       10 Mo    offset -> position dans le .log (index creux)
00000000000000000000.timeindex   10 Mo    horodatage -> offset (pour la rétention et la recherche par date)
00000000000000010738.snapshot    102 o    état des producteurs idempotents (identifiant + dernier numéro de séquence)
leader-epoch-checkpoint          16 o     historique des leaders (sert à tronquer proprement après un changement de leader)
```

* Le `.log` est découpé en **segments** : le nom du fichier est l'offset du premier message qu'il contient. Un nouveau
  segment démarre quand le segment courant atteint `segment.bytes` (1 Go) ou `segment.ms` (7 jours).
* Les index sont **creux** : une entrée tous les 4 Ko de log (`index.interval.bytes=4096`). Pour lire l'offset 5 000,
  Kafka cherche dans l'index l'entrée juste avant, puis lit le log séquentiellement à partir de là. Les fichiers d'index
  font 10 Mo parce qu'ils sont **préalloués**.
* `kafka-dump-log` montre ce qu'il y a dans un batch : `producerId: 2`, `baseSequence: 0, 1, 2…` (l'idempotence en
  action), `compresscodec: none`, `isTransactional: false`.

## 2.2 La réplication : leader, ISR, HW

Pour chaque partition et chaque réplique, Kafka suit deux positions :

* **LEO** (*Log End Offset*) : l'offset du prochain message à écrire dans **cette** réplique, c'est-à-dire jusqu'où
  elle est remplie.
* **HW** (*High Watermark*) : le plus petit LEO parmi les répliques de l'ISR. C'est la limite des messages **présents
  sur toutes les répliques à jour**.

```
leader    : [0][1][2][3][4][5][6]       LEO = 7
suiveur A : [0][1][2][3][4][5]          LEO = 6
suiveur B : [0][1][2][3][4]             LEO = 5     HW = min(7, 6, 5) = 5
                           ^ un consommateur ne lit que jusqu'au HW (offsets 0 à 4)
```

* Les consommateurs ne lisent **que jusqu'au HW**. On ne leur montre jamais un message qui pourrait disparaître si le
  leader mourait.
* Un suiveur **sort de l'ISR** s'il n'a pas rattrapé le leader depuis `replica.lag.time.max.ms` (30 s), et y **rentre**
  dès qu'il l'a rattrapé : c'est ce qu'a fait kafka3 en 4 s en A2.
* Si le leader tombe, le **contrôleur** (en KRaft, un quorum de 3 contrôleurs qui votent avec l'algorithme Raft) choisit
  un nouveau leader **dans l'ISR**. Avec `unclean.leader.election.enable=false`, il refuse de choisir une réplique hors
  ISR, parce qu'elle n'a pas tous les messages acquittés.
* Le quorum de 3 contrôleurs tolère la perte d'**un** contrôleur, puisqu'il faut une majorité de 2. En A2, kafka3 était
  le contrôleur leader (`LeaderId: 3`) : les deux autres en ont élu un nouveau.

**`acks=all` + `min.insync.replicas=2`**, mis bout à bout : le leader attend que **toutes** les répliques de l'ISR aient
le message, et refuse d'écrire si l'ISR compte moins de **2** répliques. Un message acquitté existe donc sur au moins
2 brokers. Si l'un meurt, l'autre est dans l'ISR, devient leader, et rien n'est perdu.

## 2.3 La rétention : comment elle marche vraiment

* La rétention supprime des **segments entiers, jamais des messages un par un**. Toutes les 5 minutes
  (`log.retention.check.interval.ms`), Kafka supprime les segments **fermés** dont le message le plus récent est plus
  vieux que `retention.ms`.
* Le **segment actif** (celui en cours d'écriture) n'est **jamais** supprimé. Une donnée peut donc rester **plus
  longtemps** que la rétention : jusqu'à la rétention, plus le temps de remplir le segment. Dans notre
  dimensionnement, une partition reçoit 20 / 12 = 1,67 Mo/s et remplit 1 Go en environ 11 minutes, ce qui est précis.
  Sur un topic **peu actif**, un segment peut rester ouvert 7 jours (`segment.ms`) : il faut alors baisser `segment.ms`.
* `retention.bytes` limite la taille **par partition** (et non par topic). Taille max d'un topic = `retention.bytes` ×
  partitions × RF. C'est un garde-fou contre le disque plein, les deux limites s'appliquant ensemble.
* La rétention se fonde sur l'**horodatage** des messages (`.timeindex`). En `CreateTime`, c'est l'horloge du producteur :
  un producteur à l'heure fausse peut faire garder des données trop longtemps ou trop peu.
* **Vérifié sur le cluster** : le topic `commandes` a été créé sans réglage de rétention, et il a donc **7 jours**
  (`retention.ms=604800000`, valeur par défaut), pas 3. Il faut la fixer explicitement à la création :
  `--config retention.ms=259200000`.
* **Compaction** (`cleanup.policy=compact`) : au lieu de supprimer par âge, Kafka garde **la dernière valeur de chaque
  clé**. Utile pour un état (profil client, dernier CA de chaque minute) ; inutile pour `commandes`, où chaque événement
  compte. Les offsets des consommateurs (`__consumer_offsets`) sont stockés dans un topic compacté.

**Pourquoi 3 jours ?** La rétention est la **fenêtre de rejeu** : la durée pendant laquelle on peut revenir en arrière.

* Si le dépôt vers HDFS tombe un vendredi soir, on le relance le lundi sans rien perdre.
* Si on corrige un bug dans le job Flink, on le relance en relisant les 3 derniers jours.
* L'historique long, lui, va dans HDFS : il ne sert à rien de payer 3 copies sur des disques rapides pour des données
  de 6 mois.

**Optimiser la rétention, c'est réduire ce qu'on stocke pour la même durée** :

| Levier | Effet | Preuve ou chiffre |
|---|---|---|
| **Compression** côté producteur (lz4, zstd) | divise disque, réseau et réplication | mesuré : **×3,7 (lz4), ×6,5 (zstd)** ; 15,55 To ramenés à 4,2 ou 2,4 To |
| **Batching** (`linger.ms`, `batch.size`) | des batchs plus gros se compressent mieux et coûtent moins par message | le générateur du kit envoie 1 message par batch : 219 octets stockés pour 149 utiles |
| **Format binaire** (Avro ou Protobuf + Schema Registry) | ne répète pas les noms de champs JSON, et contrôle le schéma | |
| **Rétention par topic selon sa valeur** | 3 jours pour `commandes`, moins pour des topics de test ou de métriques | |
| **Stockage hiérarchisé** (Kafka 3.6 et plus) | les vieux segments partent sur un stockage objet bon marché (S3) | rétention plus longue sans plus de disques |
| **RF 2** pour des topics non critiques | 1 copie de moins | à réserver aux données qu'on peut régénérer |

## 2.4 L'efficacité : pourquoi Kafka est rapide

* **Écriture séquentielle** à la fin d'un fichier : c'est le mode d'accès le plus rapide d'un disque.
* **Cache de pages du système** : les messages récents restent en mémoire, et un consommateur à jour lit depuis la RAM,
  pas depuis le disque.
* **Zéro-copie** (`sendfile`) : le broker envoie les octets du fichier vers le réseau sans les recopier dans sa mémoire.
  C'est possible parce que le broker stocke les batchs **tels que le producteur les a envoyés**, compression comprise.
* **Batching de bout en bout** : le producteur regroupe, le broker stocke le groupe, le consommateur lit par paquets.
* **Partitions** : le parallélisme est horizontal, une partition par consommateur du groupe au maximum.

**Les réglages qui comptent :**

| Côté | Réglage | Compromis |
|---|---|---|
| Producteur | `linger.ms`, `batch.size` | quelques ms de latence en plus, beaucoup plus de débit et de compression |
| Producteur | `compression.type` | un peu de CPU contre disque et réseau |
| Producteur | `acks` | `all` = durable mais plus lent que `1` |
| Consommateur | `fetch.min.bytes`, `fetch.max.wait.ms`, `max.poll.records` | regrouper les lectures contre la latence |
| Topic | nombre de partitions | plus de parallélisme, mais plus de fichiers, d'élections et de petits batchs |
| Producteur | **choix de la clé** | `customer_id` garantit l'ordre par client, mais un client très actif concentre une partition (le skew existe aussi dans Kafka) |

## 2.5 Idempotence et transactions

* **Idempotence** : le producteur reçoit un **identifiant** (`producerId`) et numérote chaque batch, par partition
  (`baseSequence`). Si un renvoi arrive avec un numéro déjà vu, le broker l'ignore. Un `retry` ne crée donc plus de
  doublon, à condition de garder `max.in.flight.requests.per.connection ≤ 5`, qui préserve aussi l'ordre. Le fichier
  `.snapshot` sauvegarde ces numéros pour s'en souvenir après un redémarrage.
* **Transactions** : un producteur doté d'un `transactional.id` écrit des messages dans plusieurs partitions, puis les
  **valide** ou les **annule** d'un bloc. Un **coordinateur de transactions** (topic interne `__transaction_state`,
  visible dans la liste des topics du TP) suit leur état. À la validation, Kafka écrit un **marqueur de commit** dans
  chaque partition ; à l'annulation, un **marqueur d'abandon**.
* **LSO** (*Last Stable Offset*) : la limite avant la première transaction encore ouverte. Un lecteur `read_committed`
  ne lit que jusqu'au LSO et saute les messages des transactions annulées. Vu en B4 : le HW monte à l'écriture, le LSO
  au commit.

## 2.6 Questions de la prof sur Kafka

**Q. C'est quoi l'ISR, concrètement, dans la sortie `--describe` ?**
La colonne `Isr` liste les brokers dont la copie de la partition est **à jour** avec le leader. Au départ, `Isr: 3,1,2`
partout : les 3 copies sont synchronisées. Après l'arrêt de kafka3, `Isr: 1,2` : sa copie ne suit plus. La colonne
`Replicas`, elle, ne change pas : c'est la liste des brokers **prévus** pour héberger la partition, pas leur état.
L'ISR compte parce que (1) `acks=all` attend toutes les répliques de l'ISR et (2) seul un membre de l'ISR peut devenir
leader sans perte.

**Q. Pourquoi l'écriture a-t-elle été refusée ? Le message est-il perdu ?**
`acks=all` impose d'écrire sur toutes les répliques de l'ISR, et `min.insync.replicas=3` exige que l'ISR en compte au
moins 3. Il n'en restait que 2 : le leader a répondu `NOT_ENOUGH_REPLICAS` **avant d'écrire**. Le message n'est pas
perdu, il est **refusé**, et le producteur reçoit une exception : l'application sait qu'il faut réessayer. Preuve :
après correction, la relecture du topic ne contient pas ce message (l'offset 0 est `commande-1`).

**Q. Pourquoi `min.insync.replicas=2` et pas 1 ?**
Avec 1, le leader peut acquitter un message qu'il est **seul** à avoir ; s'il meurt avant que les suiveurs le copient,
le message est perdu alors qu'il était confirmé. Avec 2, tout message confirmé existe sur 2 brokers. Règle :
**min.insync.replicas = RF − 1**, ce qui tolère une panne sans perte et sans blocage.

**Q. Quelle différence entre `acks` et `min.insync.replicas` ?**
`acks` est réglé par le **producteur** : « je veux attendre l'accord de qui ? ». `min.insync.replicas` est réglé sur le
**topic** : « combien de répliques à jour au minimum pour accepter ? ». L'un sans l'autre ne protège pas : `acks=all`
avec un ISR réduit au seul leader revient à `acks=1`.

**Q. Pourquoi les partitions 0 et 3 ont-elles changé de leader, et pas les autres ?**
Leur leader était kafka3 (première colonne `Leader: 3`). Le contrôleur a élu un nouveau leader **dans l'ISR** (le broker
1). Les autres partitions avaient déjà un leader vivant. Au retour de kafka3, il n'est pas redevenu leader tout de
suite : il a fallu une **élection préférée** (ou attendre le rééquilibrage automatique de Kafka, toutes les 5 minutes).

**Q. Pourquoi les 6 messages sont-ils tous dans la partition 4 ?**
Ils n'ont pas de clé. Le producteur utilise alors un partitionneur « collant » : il remplit un batch pour **une**
partition avant de passer à la suivante, pour faire des batchs plus gros. 6 petits messages tiennent dans un batch.
Avec une clé (par exemple `customer_id`), c'est le hash de la clé qui choisit la partition, et toutes les commandes d'un
client arrivent dans la même partition, dans l'ordre.

**Q. Comment as-tu choisi 12 partitions ? Et si on passe à 20 consommateurs ?**
La partition est l'unité de parallélisme : dans un groupe, chaque partition n'est lue que par **un** consommateur. Avec
10 consommateurs, il en faut au moins 10, sinon certains n'ont rien à lire. J'ai pris 12 pour garder de la marge et parce
que 12 se divise par 3 et par 4 (nombre de brokers). Pour 20 consommateurs, 12 partitions ne suffisent plus : 8
resteraient inactifs. Il faudrait passer à 24, ce qui change le hash des clés : on le planifie (nouveau topic et
migration), ou on choisit 24 dès le départ si la croissance est prévue.

**Q. Pourquoi pas 100 partitions, pour être tranquille ?**
Chaque partition coûte : fichiers ouverts et index sur chaque broker, élections plus longues en cas de panne (il faut
réélire un leader pour chacune), mémoire côté clients, et batchs plus petits donc moins bien compressés. On dimensionne
au besoin plus une marge, pas au maximum.

**Q. Tes 4 brokers, c'est justifié comment ?**
Stockage : 15,55 To divisés par 5,6 To utiles par broker (8 To remplis à 70 %) donnent 2,8, donc 3 brokers. Réseau :
100 Mo/s sortants sur 3 brokers font 33 Mo/s chacun, bien sous une carte de 125 Mo/s. Le stockage est donc la contrainte.
J'ajoute 1 broker pour pouvoir en arrêter un (panne ou maintenance) sans que le suivant bloque tout. Ce sont des
**hypothèses**, écrites comme le demande l'énoncé ; avec des disques de 12 To, 3 brokers suffiraient.

**Q. Ton calcul de stockage tient-il compte de la compression ?**
Non, c'est le cas le plus défavorable : 1 Ko par message, non compressé. J'ai mesuré l'effet sur le cluster avec les
mêmes 30 000 commandes : lz4 divise par 3,7 et zstd par 6,5. Avec zstd, il faudrait environ 2,4 To au lieu de 15,55 To.
Le dimensionnement non compressé reste prudent, et la compression donne une marge.

**Q. Le topic `commandes` garde-t-il bien 3 jours ?**
Non, et je l'ai vérifié : sans réglage à la création, il hérite de la valeur par défaut, 7 jours. Il faut la fixer sur
le topic (`retention.ms=259200000`). Même ainsi, la suppression se fait par segments fermés, vérifiés toutes les
5 minutes : une donnée peut rester un peu plus que 3 jours, jamais moins.

**Q. Comment Kafka garantit-il l'ordre ?**
L'ordre n'est garanti **qu'à l'intérieur d'une partition**. Pour avoir l'ordre par client, on met `customer_id` en clé :
même clé, même partition. Il n'y a pas d'ordre global entre partitions. Avec l'idempotence, les renvois ne cassent pas
cet ordre.

**Q. KRaft, ça change quoi par rapport à ZooKeeper ?**
Les métadonnées du cluster (topics, partitions, leaders, ISR) sont stockées dans Kafka lui-même, dans un journal
répliqué par un quorum de contrôleurs qui votent avec Raft, au lieu d'un cluster ZooKeeper à part. C'est un système de
moins à opérer, et un basculement plus rapide. Dans le kit, chaque broker est aussi contrôleur
(`KAFKA_PROCESS_ROLES: broker,controller`).

**Q. Que sont les colonnes `Elr` et `LastKnownElr` ?**
Elles viennent d'une évolution récente de Kafka (KIP-966, *Eligible Leader Replicas*). L'objectif : garder une liste de
répliques sûres pour devenir leader, même quand l'ISR descend sous `min.insync.replicas`. La fonctionnalité n'est pas
activée dans ce cluster, d'où les colonnes vides.

# 3. Flink en profondeur

## 3.1 Le temps et les watermarks

* **Temps d'événement** : l'heure écrite dans l'événement (`ts`). **Temps de traitement** : l'horloge de Flink. On
  calcule en temps d'événement pour que le résultat ne dépende ni des retards ni du moment où on relance le traitement.
* Problème : en temps d'événement, Flink ne sait jamais si un événement plus ancien va encore arriver. Le **watermark**
  tranche : W = plus grand `ts` vu − tolérance. C'est la promesse « plus rien d'antérieur à W ».
* Le watermark est **calculé par partition Kafka**, puis Flink retient le **minimum** sur toutes ses entrées. Une
  partition sans données bloque donc tout le monde (problème de la **source inactive** ; réglage
  `table.exec.source.idle-timeout`).
* Il est émis périodiquement (toutes les 200 ms par défaut).
* Quand W dépasse la fin d'une fenêtre, Flink **émet** le résultat et **purge** l'état de la fenêtre. Un événement qui
  arrive ensuite pour cette fenêtre est **ignoré** (en SQL, avec les fonctions de fenêtre).

## 3.2 L'état

* Une agrégation par fenêtre garde, **pour chaque clé et chaque fenêtre ouverte**, un **accumulateur** (ici le nombre et
  la somme) et un **timer** qui déclenchera l'émission.
* L'état est réparti entre les sous-tâches par **groupes de clés** (*key groups*, 128 par défaut) : chaque client est
  toujours traité par la même sous-tâche. C'est pour ça qu'un checkpoint contient un fichier par sous-tâche (262 et
  303 clients).
* **Vérifié** : l'état du job d'alertes suit une courbe en **dents de scie**. Il monte de 40 à 98 Ko pendant la minute,
  au fil des nouveaux clients, puis retombe juste après xx:05, quand la fenêtre est émise et purgée.
* Une agrégation **sans fenêtre** (`GROUP BY customer_id` tout court) garde son état **pour toujours** ; il faut alors un
  **TTL** (`table.exec.state.ttl`) pour le purger. C'est le sujet de `04_etat_ttl.sql`.
* **Où vit l'état** (*state backend*) :
  * `HashMapStateBackend` (celui du TP) : en mémoire Java. Très rapide, mais limité par la RAM, et chaque checkpoint
    copie tout l'état.
  * `EmbeddedRocksDBStateBackend` : sur le disque local. Supporte des états énormes et des **checkpoints
    incrémentaux** : on n'envoie que les fichiers nouveaux, rangés dans le répertoire `shared/`, vide dans le TP.

## 3.3 Les checkpoints : de quoi ils sont faits

**Objectif.** Prendre une photo **cohérente** de tout un job réparti sur plusieurs machines, **sans l'arrêter**, pour
pouvoir repartir après une panne comme si elle n'avait pas eu lieu.

**Comment la photo est prise (algorithme de Chandy-Lamport, avec des « barrières ») :**

1. Toutes les 10 s, le **coordinateur de checkpoints** du JobManager déclenche le checkpoint n.
2. Chaque **source** note sa position (les offsets Kafka de ses partitions), puis injecte une **barrière n** dans le flux.
   La barrière sépare « avant le checkpoint » et « après le checkpoint ».
3. Chaque **opérateur** qui a reçu la barrière n sur **toutes** ses entrées (c'est l'**alignement**) sauvegarde son
   état, puis fait suivre la barrière. La copie de l'état se fait en arrière-plan : le traitement continue.
4. Le **puits** Kafka transactionnel, à la barrière, vide son tampon et **prépare** sa transaction.
5. Chaque tâche confirme au JobManager. Quand **toutes** ont confirmé, le checkpoint est **terminé** : le fichier
   `_metadata` est écrit.
6. Le JobManager notifie les tâches, et le puits **valide** la transaction Kafka (deuxième phase du commit en deux
   phases).

**Ce qu'il contient (vérifié, checkpoint n°20 du job d'alertes, 88 Ko) :**

| Élément | Où | Vu dans les fichiers |
|---|---|---|
| Offsets Kafka de chaque sous-tâche source | `_metadata` (petit état, stocké directement) | `SourceReaderState`, `commandes`, `SPLIT_DISTRIBUTE` |
| Watermark de la source | `_metadata` | `watermark` |
| Accumulateurs des fenêtres ouvertes, par client | un fichier par sous-tâche (≈ 40 Ko) | 262 et 303 identifiants `C0xxxxx` |
| Timers de fin de fenêtre | mêmes fichiers | `_timer_state/event_window-timers` |
| État du writer Kafka + transactions à valider | `_metadata` | `writer_raw_states`, `streaming_committer_raw_states`, `alertes-exam-eos` |
| Description des types (pour relire l'état) | partout | `RowDataSerializer`, `LongSerializer`… |

Taille par opérateur : **726 octets** pour la source (des offsets), **87,6 Ko** pour l'agrégat et le puits.

**Ce qu'il ne contient PAS** : les commandes elles-mêmes. Elles restent dans Kafka, qui les rejouera. Un checkpoint ne
marche donc qu'avec une **source rejouable**. Il ne contient pas non plus le code du job.

**Où il est stocké** (*checkpoint storage*) : ici `file:///flink-data/checkpoints`, un volume partagé entre le
JobManager et le TaskManager. En production, HDFS ou S3, pour survivre à la perte d'une machine. Les états de moins de
20 Ko sont rangés directement dans `_metadata`. Seul le **dernier** checkpoint est conservé (`num-retained=1`), et il est
supprimé si on annule le job (`delete_on_cancellation`).

**Réglages vérifiés** : mode `exactly_once`, intervalle 10 s, délai maximum 10 min, 1 checkpoint à la fois, alignés,
durée réelle 21 à 137 ms.

**Aligné ou non aligné.** En mode aligné, un opérateur à deux entrées **attend** la barrière sur la plus lente, et
retient l'autre entrée pendant ce temps. Sous forte charge (*backpressure*), le checkpoint ralentit. Le mode **non
aligné** laisse passer la barrière tout de suite et sauvegarde aussi les messages en transit : checkpoint plus rapide,
mais plus gros.

**Checkpoint ou savepoint :**

| | Checkpoint | Savepoint |
|---|---|---|
| Déclenché par | Flink, automatiquement | l'utilisateur, à la main |
| But | reprise après **panne** | **mise à jour**, changement de parallélisme, migration |
| Format | optimisé, éventuellement incrémental | standard, portable |
| Durée de vie | Flink le supprime quand il n'en a plus besoin | gardé tant que l'utilisateur le veut |

**Choisir l'intervalle** (ici 10 s) :

| Plus court | Plus long |
|---|---|
| moins de données à relire après une panne | moins de surcharge (copies d'état, écritures) |
| exactly-once plus réactif (les résultats deviennent visibles à chaque checkpoint) | adapté aux gros états |

En production, on voit souvent 30 s à quelques minutes, sauf si l'exactly-once impose une faible latence.

## 3.4 La reprise après panne (ce qu'on a vu en B4)

1. Le TaskManager disparaît : ses tâches passent en `FAILED`, le job en **RESTARTING**.
2. Flink réessaie avec un **délai croissant** (stratégie par défaut quand les checkpoints sont actifs). Sans slot
   disponible, chaque essai échoue (`NoResourceAvailableException`) : `Restored: 8` dans l'interface.
3. Le TaskManager revient : Flink recharge le **dernier checkpoint terminé**, repositionne les sources sur les offsets
   sauvegardés et **relit** tout ce qui a suivi.
4. L'**état** est exact, comme si de rien n'était. Mais tout ce que le job avait **envoyé dehors** depuis ce
   checkpoint est **renvoyé**, d'où les doublons, sauf avec un puits transactionnel.

## 3.5 Les garanties de livraison

| Garantie | Signification | Comment |
|---|---|---|
| au plus une fois | rien n'est rejoué : perte possible | pas de checkpoint |
| **au moins une fois** | rien n'est perdu, doublons possibles | checkpoints + puits simple (défaut du connecteur Kafka) |
| **exactement une fois** | ni perte ni doublon **dans le résultat visible** | checkpoints + source rejouable + puits transactionnel (ou idempotent) + lecteurs `read_committed` |

**Le piège classique.** « Exactly-once » ne veut pas dire que chaque événement est **traité** une seule fois : après une
panne, Flink **retraite** les événements rejoués. Cela veut dire que **l'effet** est le même que s'ils avaient été traités
une fois.

**Le commit en deux phases avec Kafka (vérifié) :**

* Entre deux checkpoints, le puits écrit les résultats dans une **transaction ouverte**. Ils sont physiquement dans le
  topic (le HW monte à 00:22:05,6), mais **invisibles** en `read_committed`.
* À la barrière, il **prépare** la transaction et range son identifiant dans le checkpoint
  (`streaming_committer_raw_states`).
* Quand le checkpoint est terminé (00:22:07,4), il **valide** : le LSO rattrape le HW à 00:22:07,6, et Kafka écrit les
  **marqueurs de commit** (les 2 offsets de plus).
* En cas de panne avant la validation, la transaction est **annulée** à la reprise. Ses messages **restent dans le
  log** : 654 alertes pour 21:07 en `read_uncommitted`. Mais un lecteur `read_committed` les saute : il n'en voit que
  327.
* Après la reprise, les transactions déjà préparées dans le checkpoint restauré sont **revalidées** (valider deux fois
  ne change rien), et la fenêtre en cours est recalculée dans une nouvelle transaction.

**Le piège du délai de transaction.** Si Flink reste arrêté plus longtemps que `transaction.timeout.ms`, Kafka annule de
lui-même une transaction préparée mais pas encore validée, et ces résultats sont perdus. Il faut donc un délai supérieur
à la durée maximale d'une panne. Le kit autorise 1 h côté broker (`KAFKA_TRANSACTION_MAX_TIMEOUT_MS`) ; j'ai mis 15 min.

## 3.6 Optimisations Flink à connaître

* **Agrégation en deux phases** (vue dans le graphe : `LocalWindowAggregate` puis `GlobalWindowAggregate`) : chaque
  sous-tâche pré-agrège avant d'envoyer. Moins de réseau, et une clé très fréquente pèse moins (antidote au skew).
* **Mini-batch** (`06_skew_minibatch.sql`) : regrouper les mises à jour de l'état sur quelques secondes pour moins
  d'accès à l'état.
* **Parallélisme** : jamais plus de sous-tâches source que de partitions Kafka. Ici 2 sous-tâches pour 6 partitions,
  soit 3 chacune.
* **TTL d'état** pour les agrégations sans fenêtre ; **RocksDB + checkpoints incrémentaux** pour les gros états.
* **`idle-timeout`** pour qu'une partition vide ne bloque pas le watermark.
* **Surveiller** : retard des consommateurs (*lag*), *backpressure*, durée et taille des checkpoints.

## 3.7 Questions de la prof sur Flink

**Q. Qu'est-ce qu'un watermark, avec tes mots ?**
C'est la position de l'horloge **des événements** dans Flink : « j'ai vu des événements jusqu'à telle heure, j'estime que
plus rien d'antérieur à cette heure moins 5 s n'arrivera ». Quand cette horloge dépasse la fin d'une minute, Flink publie
la minute. Le délai du watermark, c'est **combien de temps on accepte d'attendre les retardataires**.

**Q. Pourquoi perds-tu 2,6 % avec 5 s ? Ce chiffre vient d'où ?**
Il se calcule. 10 % des événements sont retardés de 1 à 40 s. Un événement de la seconde s, retardé de d, est perdu si
s + d > 65 (fin de minute + 5 s). Avec s et d uniformes, cela arrive pour 26,2 % des événements retardés, soit
10 % × 26,2 % = **2,62 %**. On a mesuré entre 2,2 et 2,8 %. Avec 30 s, le même calcul donne 0,21 % (mesuré : 0,2 %) ; avec
60 s, 0 %, car un retard maximal de 40 s ne peut pas dépasser 60 s.

**Q. Pourquoi le délai d'émission vaut-il exactement le watermark ?**
La minute 20:52 est publiée quand le watermark atteint 20:53:00, donc quand Flink voit un événement daté de 20:53:05.
Les événements arrivent en temps réel, donc cela se produit vers 20:53:05 à l'horloge : 5,6 s mesurées, le reste étant le
traitement et l'écriture.

**Q. Et la minute 20:49, émise après 32,8 s avec 5 s et avec 30 s ?**
C'est le démarrage. Le générateur a commencé à 20:50:32, et la minute 20:49 ne contient que 5 événements, tous retardés.
Il n'y avait aucune donnée avant 20:50:32, donc le watermark n'a pas pu avancer : il a sauté d'un coup au premier événement
en temps réel, ce qui a publié 20:49 à 20:50:32 avec les deux réglages. Avec 60 s, il fallait atteindre 20:51:00. C'est
aussi pour ça que 4 des 5 événements de 20:49 sont perdus : ils sont arrivés après ce saut.

**Q. Comment as-tu mesuré le délai d'émission ?**
Les topics de sortie sont en `message.timestamp.type=LogAppendTime` : c'est le broker qui horodate chaque résultat à
l'arrivée, pas Flink. Délai = horodatage Kafka − fin de la minute. La référence « réel » vient d'une relecture complète du
topic `commandes`, minute par minute.

**Q. Qu'y a-t-il dans un checkpoint ?**
Trois choses, vérifiées dans les fichiers : (1) la **position de lecture** des sources, c'est-à-dire les offsets Kafka et le
watermark ; (2) l'**état des opérateurs**, c'est-à-dire les accumulateurs de chaque fenêtre ouverte, client par client, et
les timers ; (3) l'**état du puits**, c'est-à-dire les transactions Kafka préparées à valider. Plus un fichier
`_metadata` qui sert d'index. Les données elles-mêmes n'y sont pas : Kafka les garde.

**Q. Comment Flink prend-il une photo cohérente d'un job qui tourne sur plusieurs machines ?**
Avec des **barrières** : les sources notent leurs offsets et injectent une barrière dans le flux. Chaque opérateur
sauvegarde son état quand la barrière est passée sur toutes ses entrées, puis la fait suivre. Tout ce qui est avant la
barrière est dans la photo, rien de ce qui est après n'y est. Le traitement ne s'arrête pas : la copie se fait en
arrière-plan.

**Q. Pourquoi 0 doublon au premier essai de panne et 338 au second ?**
Ce qui est rejoué, c'est tout ce qui suit le **dernier checkpoint terminé**. Au premier essai, le checkpoint s'est
terminé juste **après** l'émission des alertes : elles faisaient partie de la photo, rien n'a été rejoué. Au second,
la panne est tombée **entre** l'émission et le checkpoint : la reprise est partie d'un état d'avant l'émission, et la
minute a été recalculée et réécrite. Les doublons dépendent donc du hasard du moment de la panne ; c'est pour ça qu'on
veut une garantie.

**Q. Comment vérifies-tu les doublons ?**
Je définis l'identité d'une alerte : `(customer_id, w_start)`, puisqu'un client ne peut avoir qu'une alerte par minute.
Je relis **tout** le topic et je compte les clés vues plus d'une fois, minute par minute. Pour le topic exactly-once, je le
fais deux fois : en `read_uncommitted` (tout ce qui est dans le log) et en `read_committed` (ce que voit un lecteur normal).

**Q. Ton exactly-once, il coûte quoi ?**
(1) **Latence** : les résultats sont visibles au checkpoint suivant ; 9,1 s mesurées au lieu de 5,5 s, jusqu'à 10 s de
plus dans le pire cas. (2) **Charge** : transactions, marqueurs, coordinateur. (3) **Contraintes** : tous les lecteurs
doivent passer en `read_committed`, sinon ils voient les transactions annulées (on l'a vu : 654 alertes au lieu de 327).
(4) **Risque** : une panne plus longue que le délai de transaction fait perdre la transaction préparée.

**Q. Existe-t-il une solution moins chère ?**
Oui : rester en at-least-once et rendre l'écriture **idempotente**. Si l'alerte a une clé `(customer_id, w_start)` et
qu'on écrit en **upsert** (topic compacté, ou table où l'on écrase par clé), un doublon remplace la version précédente au
lieu de s'ajouter. Pas de transaction, pas de latence en plus.

**Q. Pourquoi le job est-il resté en RESTARTING au lieu de repartir ?**
Pour exécuter des tâches, il faut des **slots**, et ce sont les TaskManagers qui les fournissent. Le seul TaskManager était
arrêté : 0 slot. Flink réessaie à intervalles croissants et échoue (`NoResourceAvailableException`) tant qu'aucun
TaskManager ne revient. En production, on a plusieurs TaskManagers, ou un orchestrateur (Kubernetes) qui en relance un.

**Q. Pourquoi le CA par minute a-t-il un parallélisme de 1, et les alertes de 2 ?**
Le CA par minute n'a pas de clé : un seul total par minute, donc une seule tâche pour la phase finale. Les alertes sont
regroupées par `customer_id` : les clients sont répartis entre 2 sous-tâches, et chacune gère sa moitié.

**Q. Pourquoi le seuil « plus de 3 commandes » donne-t-il autant d'alertes ?**
Le calcul le prévoit. 50 commandes/s × 60 s × 97 % de montants valides, sur 1 000 clients, font 2,91 commandes par client
et par minute en moyenne. Avec une loi de Poisson, P(X > 3) = 33,3 %, soit 333 alertes par minute ; on en observe 324 à
338. La règle est mal calibrée : il faudrait un seuil plus haut, ou relatif au comportement habituel du client.

**Q. Tu as choisi 5 s pour la fraude : et les fraudes que tu rates ?**
On rate les événements arrivés plus de 5 s après la fin de leur minute (2,6 % ici). Pour une alerte, la vitesse vaut plus
que l'exhaustivité, et le batch Spark rattrape tout le lendemain. On peut aussi envoyer les retardataires vers une
sortie séparée, analysée à part.

# 4. HDFS en profondeur

## 4.1 Les mécanismes

* **NameNode** : garde en **RAM** l'arborescence (fichiers, répertoires) et la liste des blocs de chaque fichier
  (environ 150 octets par objet). Il les rend durables avec un **fsimage** (une photo complète) et un **journal des
  modifications** (*edits*), fusionnés régulièrement. Cette fusion s'appelle aussi un **checkpoint**, au sens HDFS.
* **L'emplacement des blocs n'est pas sauvegardé** : au démarrage, chaque DataNode envoie la liste de ses blocs. Avec
  des millions de petits fichiers, le redémarrage est long.
* **DataNodes** : stockent les blocs (128 Mo) et envoient un battement de cœur toutes les 3 s. Un DataNode silencieux
  pendant environ 10 minutes est déclaré mort, et ses blocs sont recopiés ailleurs pour revenir à 3 copies.
* **Écriture en pipeline** : le client écrit au 1^er^ DataNode, qui transmet au 2^e^, qui transmet au 3^e^ ; les
  accusés remontent la chaîne. **Lecture** : le client demande au NameNode où sont les blocs, puis lit directement chez
  le DataNode le plus proche.
* **Placement des copies selon les baies** : 1^re^ copie sur le nœud qui écrit, 2^e^ sur une **autre baie**, 3^e^ sur
  la même baie que la 2^e^. Une baie entière peut tomber sans perte.
* **Pourquoi 128 Mo** : moins de blocs donc moins de métadonnées, des lectures longues et séquentielles, et une tâche
  Spark par bloc d'une taille raisonnable. HDFS est fait pour **peu de très gros fichiers, écrits une fois, lus en
  entier**.
* **Codes d'effacement** (Hadoop 3) : pour les données froides, RS(6,3) stocke 6 blocs de données + 3 de parité, soit
  1,5 fois le volume au lieu de 3, et tolère la perte de 3 blocs. En échange, la lecture et la reconstruction sont plus
  lentes.

## 4.2 Questions de la prof sur HDFS

**Q. Que signifie `Live_repl=1` dans ton fsck ? C'est grave ?**
C'est le nombre de copies **vivantes** du bloc : 1. Ce n'est pas une anomalie, puisque `Default replication factor: 1` :
avec un seul DataNode, on ne peut pas faire deux copies sur deux machines. C'est la configuration de Colab. Si on avait
demandé 3 copies sur un seul DataNode, fsck afficherait 100 % de blocs **sous-répliqués**. En production : 3 DataNodes au
moins, sur 2 baies.

**Q. Pourquoi les petits fichiers posent-ils problème s'ils ne prennent pas de place ?**
Parce que la limite de HDFS n'est pas le **volume**, c'est le **nombre d'objets** que le NameNode garde en RAM : 150 octets
par fichier et par bloc, qu'il pèse 1 Ko ou 128 Mo. Mes 21 fichiers de 48 Ko coûtent 42 objets ; le même contenu en un
fichier en coûte 2. Sur 6 mois, cela donne 7,9 Go de RAM pour un seul NameNode, plus un appel au NameNode pour chaque
ouverture, et une tâche Spark par fichier.

**Q. Ton calcul de 7,9 Go, d'où vient le facteur 2 ?**
Chaque fichier de moins de 128 Mo tient dans **1 bloc**. 1 fichier = 1 objet fichier + 1 objet bloc = 2 objets.
26 208 000 fichiers × 2 × 150 octets = 7,86 Go.

**Q. Comment divises-tu par 60 ?**
On passe d'un fichier par minute à un fichier par heure, et il y a 60 minutes dans une heure : 436 800 fichiers et
131 Mo de RAM au lieu de 7,9 Go. Mieux encore : des fichiers dimensionnés par leur **taille** (128 Mo à 1 Go) et
partitionnés par date et heure, l'entrepôt devenant une colonne.

**Q. À quoi servent les zones raw, curated, rejects et analytics ?**
`raw` : les données **brutes et immuables**, pour pouvoir tout recalculer si une règle change. `curated` : les données
nettoyées, utilisables. `rejects` : la **quarantaine**, chaque ligne écartée avec son motif, pour pouvoir expliquer et
corriger à la source. `analytics` (gold) : les agrégats pour la BI. C'est l'organisation « médaillon » (bronze, silver,
gold).

**Q. Pourquoi `ingestion_date=2026-11-02` dans le chemin ?**
C'est la convention de partitionnement « Hive » : Spark reconnaît `ingestion_date` comme une colonne. Une requête
filtrée sur une date ne lit que ce répertoire (*partition pruning*), au lieu de tout le stockage.

**Q. Comment déposer Kafka vers HDFS sans recréer le problème ?**
Accumuler avant d'écrire : un connecteur (Kafka Connect HDFS Sink, ou le puits fichier de Flink) ferme un fichier quand
il atteint 128 Mo à 1 Go **ou** au bout d'une heure, au premier des deux. On partitionne par date et heure, on écrit en
Parquet compressé, et une compaction nocturne fusionne ce qui reste trop petit. Kafka, avec ses 3 jours de rétention,
sert de tampon.

# 5. Spark en profondeur

## 5.1 Les mécanismes

* **Driver et exécuteurs** : le driver construit le plan et distribue les tâches ; les exécuteurs les exécutent. En
  `local[4]`, ce sont 4 fils d'exécution sur une seule machine.
* **Évaluation paresseuse** : `filter`, `join` ou `groupBy` ne font rien tout de suite ; ils construisent un plan. Seule
  une **action** (`count`, `write`, `collect`) déclenche un **job**.
* Un job se découpe en **étapes** (*stages*) à chaque **shuffle**, et une étape en **tâches**, une par partition de
  données. **La durée d'une étape est celle de sa tâche la plus lente** : c'est pour ça que le skew coûte cher.
* **Catalyst** (l'optimiseur) : il analyse le plan, l'optimise (pousse les filtres vers la lecture, ne lit que les
  colonnes utiles), choisit la stratégie physique (le type de jointure, d'après la taille estimée des tables), puis génère
  du code compilé (`*(n)` dans le plan : *WholeStageCodegen*).
* **Shuffle** (`Exchange` dans le plan) : chaque tâche écrit ses lignes, rangées par partition de destination, sur son
  disque local ; les tâches suivantes les récupèrent par le réseau. Cela coûte disque + réseau + sérialisation : c'est
  l'opération la plus chère.
* **Stratégies de jointure** :
  * *Broadcast hash join* : la petite table (moins de 10 Mo) est envoyée à tout le monde, et la grosse **ne bouge pas**.
  * *Sort-merge join* : les deux tables sont redistribuées par la clé et triées, puis fusionnées. C'est le choix par
    défaut entre deux grosses tables.
  * *Shuffle hash join* : les deux tables sont redistribuées, puis une table de hachage est construite pour le côté le
    plus petit.
* **AQE** (*Adaptive Query Execution*, active par défaut) : réoptimise **pendant** l'exécution, avec les vraies tailles
  mesurées après chaque shuffle. Elle **regroupe** les petites partitions (vu : `AQEShuffleRead coalesced`), **change**
  un sort-merge en broadcast si une table s'avère petite, et **découpe** les partitions déséquilibrées (au-delà de 256 Mo
  et 5 fois la médiane, valeurs vérifiées).
* **Parquet** : format **en colonnes**, typé, compressé, avec des statistiques min/max par bloc. Spark ne lit que les
  colonnes utiles (vu dans le plan : `ReadSchema: struct<id_produit, montant>`) et saute les blocs exclus par un filtre
  (`PushedFilters: [IsNotNull(id_produit)]`). Les 11 449 commandes propres tiennent en 400 Ko de Parquet, contre 1 Mo de
  CSV pour les 12 484 lignes brutes.

## 5.2 Questions de la prof sur Spark

**Q. Pourquoi lire toutes les colonnes en texte ?**
Pour **voir** les erreurs. Si Spark devine le type, `2026-02-30` devient NULL silencieusement, et on ne distingue plus
une date vide d'une date invalide. En lisant en texte puis en convertissant moi-même, je compte les conversions qui
échouent : 88 dates invalides, et 12 vides en plus.

**Q. Pourquoi un seul `agg` pour le contrôle qualité ?**
Chaque action Spark relit les données. Dix `count()` = dix lectures. Un seul `agg` avec dix expressions = une lecture.
Chaque condition est transformée en 0 ou 1, et `sum` compte les 1.

**Q. Pourquoi `row_number()` plutôt que `dropDuplicates` ?**
`dropDuplicates("id_commande")` garde une copie **au hasard**, qui peut changer d'une exécution à l'autre.
`row_number()` sur une fenêtre par `id_commande`, triée par `ts_ingestion`, garde **toujours** la première arrivée : le
résultat est reproductible et la règle est explicite. Le coût est un shuffle par `id_commande` et un tri.

**Q. Comment prouves-tu que propres + rejets = commandes distinctes ?**
Je **relis les deux dossiers Parquet écrits sur disque**, pas les DataFrames en mémoire, et je compare au
`countDistinct(id_commande)` du **fichier brut** : 11 449 + 658 = 12 107. Une assertion dans le notebook bloque
l'exécution si l'égalité est fausse. Je vérifie aussi qu'aucun `id_commande` n'apparaît deux fois dans les propres.

**Q. Pourquoi 4 Exchange dans ton plan ?**
Deux pour la jointure sort-merge (chaque table est redistribuée par `id_produit`, pour que les lignes d'un même produit se
retrouvent ensemble), un pour le `groupBy("famille")`, et un pour le tri final (`orderBy`, qui demande une répartition par
plages de valeurs).

**Q. Comment as-tu prouvé le skew ?**
Trois mesures : (1) par produit, P007 fait 40,3 % des lignes, contre 286 en moyenne pour un produit ; (2) par partition,
après `repartition('id_produit')`, la plus grosse fait 25,8 fois la médiane, et 162 partitions sur 200 sont vides ;
(3) dans l'API de Spark, la tâche de jointure la plus chargée traite 461 101 lignes contre 17 851 pour la tâche médiane.

**Q. Pourquoi seulement 38 partitions non vides sur 200 ?**
Il n'y a que 40 valeurs de clé. Chaque ligne va dans la partition `hash(id_produit) mod 200` : au mieux 40 partitions
pleines, et 2 paires de produits sont tombées dans la même (collision de hash). Les 162 autres sont des tâches pour
rien : c'est aussi pour ça que l'AQE les regroupe.

**Q. Pourquoi le broadcast plutôt que le salting ?**
La table produits fait 40 lignes : l'envoyer à chaque tâche ne coûte rien, et les commandes **ne sont plus
redistribuées du tout**. Il n'y a plus de partition « P007 », le skew disparaît. Le salting coupe la clé chaude en N
morceaux, mais oblige à **dupliquer N fois** la table produits et à écrire plus de code ; il ne vaut que si les **deux**
tables sont trop grosses pour un broadcast.

**Q. Si le broadcast est si évident, pourquoi Spark ne l'a-t-il pas fait ?**
Il l'aurait fait : je l'ai vérifié. Avec les réglages par défaut, il choisit seul le `BroadcastHashJoin`, puisqu'il
estime la table produits à 1 425 octets, sous le seuil de 10 Mo. L'énoncé demandait de **désactiver** broadcast et AQE
pour faire apparaître le problème qu'on aurait entre deux grosses tables.

**Q. Ta mesure de durée sur 11 449 lignes ne veut rien dire, non ?**
C'est vrai, et c'est pour ça que j'ai mesuré sur une copie ×100 (1 144 900 lignes, même répartition). Même là, la durée
absolue dépend de la machine. Les mesures **robustes** sont ailleurs : 1 144 980 lignes envoyées en shuffle contre 20, et
un déséquilibre de 25,8 contre 1,0. Celles-là ne dépendent pas du matériel.

**Q. Pourquoi `coalesce(1)` à l'écriture ? Ce n'est pas mauvais pour le parallélisme ?**
Pour 11 449 lignes (400 Ko), plusieurs fichiers seraient de **petits fichiers** (partie C). `coalesce(1)` réduit le
nombre de partitions sans shuffle, au prix de moins de parallélisme dans l'étape qui écrit. Sur de gros volumes, on
viserait plutôt des fichiers de 128 Mo à 1 Go (`repartition` selon la taille, ou `maxRecordsPerFile`).

**Q. Pourquoi `lignes.cache()` ?**
Le DataFrame `lignes` sert quatre fois (distinctes, propres, rejets, réconciliation). Sans cache, Spark referait à chaque
fois la lecture du CSV, les conversions et le `row_number`. Le cache le garde en mémoire après le premier calcul.

**Q. Pourquoi `spark.sql.ansi.enabled=false` ?**
En Spark 4, le mode ANSI est actif par défaut : une conversion impossible (`cast` d'un texte en nombre, date invalide)
**fait échouer** le job. Je veux au contraire obtenir NULL et compter ces lignes. Une autre solution aurait été
`try_cast` ou `try_to_timestamp`. Je fixe aussi le parseur de dates (`CORRECTED`) pour avoir le même comportement en
Spark 3.5 (Colab) et en Spark 4.

# 6. KPI et réconciliation

**Q. Comment es-tu sûr que ta décomposition de l'écart est juste ?**
Deux contrôles indépendants. (1) Les règles Flink **rejouées** sur les données brutes redonnent **exactement**
`ca_minute_flink.csv`, avec 0 écart sur les 120 minutes, en nombre de commandes comme en montant. Mon modèle du job Flink
est donc juste. (2) Les causes sont **exclusives** : chaque ligne comptée d'un seul côté l'est pour une seule raison.
Leur somme retombe sur l'écart **au centime**, et une assertion dans le notebook le vérifie.

**Q. Pourquoi 295 doublons dans l'écart alors que D1 en compte 377 ?**
Une copie ne crée un écart que si Flink la compte. Sur 377 copies : 295 sont comptées par Flink, 63 sont arrivées trop
tard (Flink les ignore aussi) et 19 portent une anomalie (quantité, prix ou date) que Flink filtre. 295 + 63 + 19 = 377.
De même pour le pays absent : 166 + 15 tardives + 5 autres anomalies = 186.

**Q. L'écart n'est que de −4 % : est-ce grave ?**
L'écart **net** cache des écarts **bruts** presque trois fois plus gros : +3,9 % (doublons et pays) et −8,0 % (retards),
qui se compensent en partie. Minute par minute, il va de −10 555 € à +4 235 €. Un chiffre global « acceptable » peut donc
cacher des minutes très fausses, et des causes qui varient d'un jour à l'autre.

**Q. Pourquoi Spark est-il la source de vérité, et pas Flink ?**
Spark calcule **après coup** : il voit tous les événements, même ceux arrivés 2 minutes en retard. Il dédoublonne,
applique les contrôles qualité, et on peut le relancer sur les mêmes données brutes pour obtenir le même résultat.
Flink est incomplet **par construction** : son watermark écarte les retardataires. Il est irremplaçable pour la
réactivité, mais son chiffre est provisoire.

**Q. Pourquoi rattacher une commande à la date d'événement et pas à la date d'ingestion ?**
Le chiffre d'affaires d'une minute, c'est ce qui a été **vendu** pendant cette minute. Une commande passée à 08:59:58
et reçue à 09:00:40 appartient à 08:59. Rattacher à l'ingestion ferait dépendre le CA des pannes réseau.

**Q. Pourquoi diviser par les commandes distinctes dans le taux de rejet ?**
Un doublon n'est pas une commande de plus, c'est un défaut de transport. Le compter au dénominateur ferait baisser
artificiellement le taux de rejet. Les doublons ont leur propre indicateur (3,02 %).

**Q. Quelle est ta recommandation la plus importante ?**
Celle qui a le plus d'effet : les **retards** (−7,96 %). Garder un watermark court pour l'écran temps réel, mais
**corriger** les minutes quand les retardataires arrivent (sortie en upsert), ou passer à 120 s pour les usages qui
veulent la complétude : 0 % de perte, puisque le retard maximal observé est de 116 s. Ensuite, dédoublonner dans Flink
(+2,49 %) et partager les règles qualité (+1,42 %).

# 7. Architecture

**Q. Lambda ou Kappa ?**
Lambda : deux chemins, rapide et complet, avec deux codes à maintenir et des écarts entre eux (notre problème). Kappa :
un seul chemin en flux ; pour recalculer, on rejoue Kafka depuis le début de la rétention. Mon amélioration va vers
Kappa : un seul nettoyage en amont, partagé, et des agrégats corrigés au fil de l'eau dans une table qui accepte les mises
à jour.

**Q. Quels autres risques de production vois-tu ?**
Le **changement de format** des messages (un champ renommé casse Flink et Spark ; parade : Schema Registry et
compatibilité de schéma). Le **retard des consommateurs** (*lag*) qui grandit sans qu'on le voie (parade : supervision,
le kit fournit Prometheus et Grafana). Un **disque plein** sur un broker (parade : `retention.bytes`, alertes). Des
**checkpoints qui échouent** sous forte charge (parade : surveiller leur durée, checkpoints non alignés, RocksDB
incrémental). Le **skew** (parade : broadcast, pré-agrégation).

**Q. Que surveilles-tu en priorité ?**
Kafka : partitions sous-répliquées et retard des consommateurs. Flink : durée et échecs des checkpoints, *backpressure*,
redémarrages. HDFS : nombre de fichiers et mémoire du NameNode. Spark : durée des jobs et tâches anormalement longues.
Métier : taux de rejet et écart Flink/Spark quotidien.

# 8. Les limites de mon travail, et comment les assumer

**Les données ne sont pas celles de l'examen.** Le kit n'était pas disponible. J'ai généré un jeu au **même format**,
avec les **anomalies décrites dans l'énoncé**, à graine fixe. Le fichier Flink applique **exactement** les règles de
l'énoncé, et je l'ai validé en le reproduisant à 0 € près. Les chiffres changeraient avec les vrais fichiers, mais pas
la méthode : le notebook recalcule tout si on remplace les CSV.

**HDFS a tourné dans Docker, pas sur Colab.** Les commandes `hdfs dfs` et `hdfs fsck` sont identiques, et la réplication
est de 1 dans les deux cas, avec un seul DataNode.

**Les durées Spark sont mesurées sur une seule machine.** C'est pour ça que je m'appuie surtout sur les mesures
indépendantes du matériel : lignes en shuffle, déséquilibre max / médiane.

**Le premier test de panne n'a donné aucun doublon.** Ce n'est pas un échec : il montre que les doublons dépendent du
moment de la panne par rapport au checkpoint. Le second test, provoqué au bon moment, les montre.

**Les hypothèses du dimensionnement** (10 Mo/s par partition, 2 Mo/s par consommateur, 8 To par broker) sont écrites,
comme le demande l'énoncé. Avec d'autres hypothèses, les nombres changent, mais le raisonnement reste le même.

# 9. Aide-mémoire

## Les chiffres à connaître

| Partie | Chiffre | Signification |
|---|---|---|
| A1 | 20 / 40 / 60 / 100 Mo/s | entrant / réplication / consommateurs / sortant total |
| A1 | 5,18 To → 15,55 To → ≈ 22 To | brut 3 jours / répliqué / capacité à prévoir |
| A1 | 12 partitions, 4 brokers | |
| A1 | ×3,7 (lz4), ×6,5 (zstd) | gain mesuré de la compression |
| A2 | ISR 3 → 2 → 3 en 4 s | panne puis retour de kafka3 |
| B1 | 2,6 % / 0,2 % / 0 % | perte avec un watermark de 5 / 30 / 60 s (calcul et mesure concordent) |
| B1 | 5,6 s / 30,4 s / 60,7 s | délai d'émission |
| B3 | 333 attendues, 324 à 338 observées | alertes par minute (loi de Poisson, λ = 2,91) |
| B4 | 0 puis 338 doublons ; 0 en exactly-once | |
| B4 | 9,1 s au lieu de 5,5 s | coût en latence de l'exactly-once |
| B4 | 654 en read_uncommitted, 327 en read_committed | la transaction annulée est dans le log, mais invisible |
| B4 | 726 o (source) + 87,6 Ko (agrégat et puits) | contenu d'un checkpoint |
| C1 | 26,2 M fichiers, 7,9 Go ; 436 800 fichiers, 131 Mo | avant et après la stratégie horaire |
| C2 | 21 fichiers = 42 objets ; 1 fichier = 2 objets | |
| D1 | 12 484 lignes, 377 doublons, 190 pays, 149 prix, 249 quantités, 88 + 12 dates | |
| D2 | 11 449 + 658 = 12 107 | |
| D3 | P007 = 40,3 % ; ×25,8 la médiane ; 38 partitions non vides sur 200 | |
| D4 | 1,45 s → 0,73 s ; 1 144 980 → 20 lignes en shuffle ; ×25,8 → ×1,0 | |
| E1 | 3 865 513,33 € ; 5,43 % de rejet | |
| E2 | −156 165,65 € = +96 433 + 54 915 − 307 514 | |

## Les formules

* Débit = messages/s × taille. Réplication = (RF − 1) × débit entrant. Sortant = lectures complètes × débit.
* Stockage = débit × durée de rétention × RF ; capacité = stockage / taux de remplissage maximal.
* Partitions ≥ max(débit / débit par partition, débit / débit par consommateur, nombre de consommateurs).
* Objets NameNode = (fichiers + blocs) × 150 octets.
* Perte due au watermark = part d'événements retardés × P(seconde dans la minute + retard > 60 + délai du watermark).
* Écart Flink − Spark = Σ montant × (compté par Flink − compté par Spark).

## Glossaire

| Terme | Définition courte |
|---|---|
| Offset | numéro d'un message dans une partition |
| Segment | morceau de fichier d'une partition ; la rétention supprime des segments entiers |
| ISR | répliques à jour avec le leader ; seules candidates pour devenir leader |
| HW (high watermark) | dernier offset présent sur toutes les répliques de l'ISR ; limite de lecture |
| LSO (last stable offset) | limite de lecture en read_committed : avant la 1^re^ transaction ouverte |
| acks | combien de répliques doivent confirmer une écriture |
| min.insync.replicas | taille minimale de l'ISR pour accepter une écriture acks=all |
| Idempotence | un renvoi ne crée pas de doublon (identifiant de producteur + numéro de séquence) |
| Compaction | ne garder que la dernière valeur de chaque clé |
| Temps d'événement | l'heure inscrite dans l'événement |
| Watermark | horloge des événements : « plus rien d'antérieur à W » |
| Checkpoint | photo cohérente (offsets + état + transactions) pour la reprise après panne |
| Barrière | marqueur injecté dans le flux qui délimite un checkpoint |
| Savepoint | checkpoint manuel et portable, pour les mises à jour |
| State backend | là où vit l'état (mémoire ou RocksDB) |
| Commit en deux phases | préparer au checkpoint, valider quand il est confirmé |
| Slot | emplacement d'exécution fourni par un TaskManager |
| Shuffle / Exchange | redistribution des lignes par le réseau ; l'opération la plus chère |
| Skew | une clé concentre une grande part des données dans une seule tâche |
| Broadcast join | envoyer la petite table à toutes les tâches ; la grosse ne bouge pas |
| AQE | réoptimisation pendant l'exécution, avec les vraies tailles |
| Couche gold | agrégats propres et définis une fois, prêts pour la BI |
| Lambda / Kappa | deux chemins (rapide + complet) / un seul chemin en flux rejouable |
