# Sauvegarder une chaîne Lambda

**Governing idea.** Totale, incrémentale et différentielle diffèrent par leur point de comparaison, et chaque brique de la chaîne Lambda n'en utilise que certaines : on sauvegarde la source, on recalcule le reste.

**Audience and occasion.** La classe du module « Traitement de flux en temps réel » (3iL, 2026-2027), étudiants qui ont fait les TP Kafka et Flink. Exposé court pour présenter une notion, en partage d'écran sur Microsoft Teams. Après l'exposé, les étudiants doivent savoir distinguer les trois types de sauvegarde et dire lequel chaque logiciel de la chaîne utilise.

## Requirements

| Section | Contenu |
| :--- | :--- |
| **Role** | Un étudiant qui a monté et cassé lui-même la chaîne Kafka, Flink, HDFS, Spark, et qui l'explique à ses camarades. |
| **Context** | La prof a demandé de présenter les types de sauvegarde (totale, incrémentale, différentielle), surtout appliqués aux logiciels de la chaîne Lambda (checkpoints, etc.). Révision du 26/11/2026 : RPO et RTO retirés, à la demande de l'auteur (c'étaient des questions de révision, pas le sujet). |
| **Goal** | Qu'à la fin, chacun sache quel type de sauvegarde chaque brique utilise, et pourquoi silver et gold ne se sauvegardent pas. |
| **Requirements** | Distinguer les trois types par leur point de comparaison, leur coût et leur restauration ; savoir que la réplication n'est pas une sauvegarde ; savoir qu'un checkpoint Flink est complet ou incrémental ; savoir que silver et gold se recalculent depuis bronze. |
| **Format** | Un seul fichier HTML, 9 slides, environ 7 minutes. Ton respectueux, positif, professionnel. En français. |
| **Resources** | `sources/` (voir ci-dessous). |

## Narrative spine

On définit d'abord les trois types par ce qu'ils comparent, puis on les voit travailler sur une semaine d'exemple, et on les compare critère par critère. On passe ensuite à la chaîne Lambda : un tableau dit quel type chaque brique utilise, puis on détaille brique par brique dans le sens du flux, avec la panne réelle de l'exercice comme preuve qu'un checkpoint se restaure. On ferme sur la règle : sauvegarder la source, recalculer le reste.

## Selections

| Layer | This deck uses | Catalogue |
| :--- | :--- | :--- |
| Archetypes | A-10, A-05, A-04, A-06, A-04, A-08, A-09, A-10, A-14 | DESIGN-SYSTEM §3.2 |
| Disclosure | Slide 2 : ce que copie chaque type (`scope`). Slide 3 : pourquoi une totale régulière (`condition`). Slide 6 : contenu d'un checkpoint (`scope`). Slide 7 : doublons at-least-once ou exactly-once (`condition`). Slide 8 : le piège de la réplication Spark (`condition`). | DESIGN-SYSTEM §5.3 |
| Motion | Rise ; révélation dans l'ordre du temps sur les slides 2 (jour par jour), 6 (checkpoint par checkpoint) et 7 (événement par événement) : encode la chronologie ; Current sur les flèches de la slide 8 : encode le sens du flux. | DESIGN-SYSTEM §5.2 |
| Visuals | Frises avec arcs « compare à » (slide 1) ; grille semaine × fichiers (slide 2) ; tableau comparatif (slide 3) ; matrice brique × type (slide 4) ; deux colonnes argumentées (slide 5) ; suite de checkpoints (slide 6) ; chronologie (slide 7) ; schéma bronze, silver, gold (slide 8). | DESIGN-SYSTEM §4 |

## Quality bar — additions only

Partage d'écran Teams : la vidéo est compressée et la cadence d'images baisse.

- Transition entre slides `immediate` (DS-235) : une transition animée devient saccadée dans Teams.
- Texte courant à 28 unités, le haut de la fourchette DS-034.
- Aucun sens porté par un trait fin ou par une couleur seule : toujours un libellé en plus.

## Sources and the figure ledger

Sources lues dans `presentation_sauvegardes/sources/`.

| Slug | Source | What it carries |
| :--- | :--- | :--- |
| exercice-flink-panne | Exercice : panne Flink | Intervalle de checkpoint, checkpoint n° 22, heures de la panne et de la reprise, doublons |
| exercice-kafka-panne | Exercice : panne Kafka | Topic en réplication 3, min.insync 2, arrêt de kafka2, 325 confirmés et 0 erreur |
| exercice-hdfs | Exercice : HDFS | Réplication 1 sur une machine, fichier Spark sous-répliqué |
| doc-apache | Documentation Apache | Rétention Kafka 7 jours, MirrorMaker 2, checkpoints incrémentaux RocksDB, savepoints, réplication HDFS 3, snapshots, DistCp |
| modele-semaine | Modèle illustratif | La semaine d'exemple : fichiers copiés par jour, copies à restaurer |

| Figure | Value | Origin | Used on |
| :--- | :--- | :--- | :--- |
| Copies à recharger, totale | 1 | modele-semaine | 2, 3 |
| Copies à recharger, incrémentale | 4 | modele-semaine | 2 |
| Copies à recharger, différentielle | 2 | modele-semaine | 2 |
| Fichiers copiés en 3 jours (totale / incr. / diff.) | 12 / 3 / 6 | modele-semaine | 2 |
| Réplication du topic | 3 | exercice-kafka-panne | 5 |
| min.insync.replicas | 2 | exercice-kafka-panne | 5 |
| Messages confirmés pendant la panne de kafka2 | 325 / 325, 0 erreur | exercice-kafka-panne | 5 |
| Rétention Kafka par défaut | 7 jours | doc-apache | 5 |
| Intervalle des checkpoints | 10 s | exercice-flink-panne | 6 |
| Checkpoint de reprise | n° 22 | exercice-flink-panne | 7 |
| Heure du checkpoint n° 22 | 23:00:04.5 | exercice-flink-panne | 7 |
| Arrêt du TaskManager | 23:00:10 | exercice-flink-panne | 7 |
| Reprise des écritures | 23:00:45 | exercice-flink-panne | 7 |
| Interruption (23:00:45 − 23:00:10) | 35 s | exercice-flink-panne | 7 |
| Données perdues | 0 | exercice-flink-panne | 7 |
| Doublons at-least-once / exactly-once | 2 / 0 | exercice-flink-panne | 7 |
| Réplication HDFS en production | 3 | doc-apache | 8 |
| Réplication HDFS dans l'exercice | 1 | exercice-hdfs | 8 |

## Outline

| # | Archetype | Title — a claim, not a topic | Bottom line |
| :-- | :--- | :--- | :--- |
| 1 | A-10 Architecture View | Tout dépend du point de comparaison | La totale copie tout ; l'incrémentale, les changements depuis la veille ; la différentielle, depuis la dernière totale. |
| 2 | A-05 Animated Trajectory | Incrémentale légère, différentielle vite restaurée | Après une panne le jeudi, l'incrémentale demande 4 copies à recharger, la différentielle seulement 2. |
| 3 | A-04 Two-Column Ledger | Chaque type gagne sur un critère | En pratique, on combine : une totale chaque semaine, puis une incrémentale ou une différentielle chaque jour. |
| 4 | A-06 Small Multiple | Chaque brique a ses propres sauvegardes | Kafka et HDFS répliquent en continu, Flink et HDFS font de la totale et de l'incrémentale, Spark recalcule. |
| 5 | A-04 Two-Column Ledger | Kafka réplique, il ne sauvegarde pas | La réplication survit à la panne d'un broker, mais une suppression par erreur est répliquée partout. |
| 6 | A-08 Process / Flow | Flink se sauvegarde lui-même, en continu | Un checkpoint enregistre l'état et les offsets Kafka : complet avec HashMap, incrémental avec RocksDB. |
| 7 | A-09 Timeline with a Gate | 35 s d'arrêt, zéro donnée perdue | Le job est reparti du checkpoint n° 22 : aucune donnée perdue, 35 s d'interruption. |
| 8 | A-10 Architecture View | Bronze sauvegardé, silver et gold recalculés | HDFS protège bronze par réplication et snapshots ; silver et gold se reconstruisent en relançant Spark. |
| 9 | A-14 Verdict / Close | Sauvegardez la source, recalculez le reste | Une totale puis des incrémentales, réservées aux données que rien ne recalcule : Kafka et bronze. |
