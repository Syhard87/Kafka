# Sauvegarder une chaîne Lambda

**Governing idea.** Dans une chaîne Lambda, chaque brique se protège avec le mécanisme qui correspond à ce qu'elle coûterait à reconstruire : on sauvegarde la source, on recalcule le reste.

**Audience and occasion.** La classe du module « Traitement de flux en temps réel » (3iL, 2026-2027), étudiants qui ont fait les TP Kafka et Flink. Exposé court pour présenter une notion, en partage d'écran sur Microsoft Teams. Après l'exposé, les étudiants doivent savoir reconnaître les trois types de sauvegarde et dire ce que chaque brique de la chaîne protège, et comment.

## Requirements

| Section | Contenu |
| :--- | :--- |
| **Role** | Un étudiant qui a monté et cassé lui-même la chaîne Kafka, Flink, HDFS, Spark, et qui l'explique à ses camarades. |
| **Context** | La prof a demandé de présenter les types de sauvegarde (totale, incrémentale, différentielle), surtout appliqués aux logiciels de la chaîne Lambda (checkpoints, etc.). Les chiffres viennent de l'exercice intégré exécuté le 25/11/2026 et de la documentation Apache. |
| **Goal** | Qu'à la fin, chacun choisisse un mécanisme de sauvegarde en se demandant d'abord si la donnée peut être reconstruite. |
| **Requirements** | Savoir distinguer totale, incrémentale et différentielle par ce qu'elles copient et ce qu'il faut pour restaurer ; savoir que la réplication n'est pas une sauvegarde ; savoir qu'un checkpoint Flink est une sauvegarde automatique, complète ou incrémentale ; savoir que silver et gold se recalculent depuis bronze. |
| **Format** | Un seul fichier HTML, 8 slides, environ 6 à 8 minutes. Ton respectueux, positif, professionnel. En français. |
| **Resources** | `sources/` (voir ci-dessous). |

## Narrative spine

On pose d'abord le critère qui juge toute sauvegarde : ce qu'on perd et le temps pour repartir. On montre ensuite les trois types sur un exemple simple d'une semaine, pour fixer le vocabulaire et le compromis copie/restauration. Puis on applique ce vocabulaire à la chaîne : la carte d'abord (où sont les données irremplaçables), puis brique par brique, dans le sens du flux, avec la panne réelle de l'exercice comme preuve. On ferme sur la question à se poser avant de choisir.

## Selections

| Layer | This deck uses | Catalogue |
| :--- | :--- | :--- |
| Archetypes | A-10, A-05, A-10, A-04, A-08, A-09, A-10, A-14 | DESIGN-SYSTEM §3.2 |
| Disclosure | Slide 3 : le tableau de synthèse logiciel × type de sauvegarde (`instances`). Slide 5 : ce que contient un checkpoint (`scope`). Slide 6 : at-least-once ou exactly-once, les doublons (`condition`). | DESIGN-SYSTEM §5.3 |
| Motion | Rise (arrivée des éléments) ; révélation pas à pas au clic sur la slide 2 (la semaine, jour par jour : encode le temps qui passe et ce qui est copié chaque jour) et sur la slide 6 (la chronologie de la panne : encode l'ordre des événements) ; Current (flèches pointillées animées) sur la slide 3 : encode le sens du flux de données ; Pulse-once sur le chiffre clé de la slide 6. | DESIGN-SYSTEM §5.2 |
| Visuals | Grille semaine × fichiers (slide 2) ; schéma de la chaîne (slide 3) ; deux colonnes argumentées (slide 4) ; suite de checkpoints complet/incrémental (slide 5) ; chronologie avec un point de décision (slide 6) ; schéma bronze → silver → gold (slide 7). | DESIGN-SYSTEM §4 |

## Quality bar — additions only

Partage d'écran Teams : la vidéo est compressée et la cadence d'images baisse.

- Transition entre slides `immediate` (DS-235) : une transition animée devient saccadée dans Teams.
- Texte courant à 28 unités, le haut de la fourchette DS-034 : il doit rester lisible après compression.
- Aucun sens porté par un trait fin ou par une couleur seule : toujours un libellé en plus.
- Les révélations pas à pas avancent au clic du présentateur, jamais sur une minuterie.

## Sources and the figure ledger

Sources lues dans `presentation_sauvegardes/sources/`.

| Slug | Source | What it carries |
| :--- | :--- | :--- |
| exercice-flink-panne | Exercice : panne Flink | Intervalle de checkpoint, checkpoint n° 22, heures de la panne et de la reprise, doublons |
| exercice-kafka-panne | Exercice : panne Kafka | Topic en réplication 3, min.insync 2, arrêt de kafka2, 325 confirmés et 0 erreur |
| exercice-hdfs | Exercice : HDFS | Réplication 1 sur une machine, bloc de 128 Mo, fichier Spark sous-répliqué |
| doc-apache | Documentation Apache | Rétention Kafka 7 jours, MirrorMaker 2, checkpoints incrémentaux RocksDB, savepoints, réplication HDFS 3, snapshots, DistCp |
| modele-semaine | Modèle illustratif | La semaine d'exemple : fichiers copiés par jour, copies à restaurer |

| Figure | Value | Origin | Used on |
| :--- | :--- | :--- | :--- |
| Copies à restaurer, incrémentale | 4 | modele-semaine | 2 |
| Copies à restaurer, différentielle | 2 | modele-semaine | 2 |
| Fichiers copiés en 3 jours (totale / incr. / diff.) | 12 / 3 / 6 | modele-semaine | 2 |
| Copies à recharger, totale | 1 | modele-semaine | 2 |
| Réplication du topic | 3 | exercice-kafka-panne | 4 |
| min.insync.replicas | 2 | exercice-kafka-panne | 4 |
| Messages confirmés pendant la panne de kafka2 | 325 / 325, 0 erreur | exercice-kafka-panne | 4 |
| Rétention Kafka par défaut | 7 jours | doc-apache | 4 |
| Intervalle des checkpoints | 10 s | exercice-flink-panne | 5, 6 |
| Checkpoint de reprise | n° 22 | exercice-flink-panne | 6 |
| Heure du checkpoint n° 22 | 23:00:04.5 | exercice-flink-panne | 6 |
| Arrêt du TaskManager | 23:00:10 | exercice-flink-panne | 6 |
| Reprise des écritures | 23:00:45 | exercice-flink-panne | 6 |
| Interruption (RTO mesuré, 23:00:45 − 23:00:10) | 35 s | exercice-flink-panne | 6 |
| Données perdues (RPO) | 0 | exercice-flink-panne | 6 |
| Doublons at-least-once / exactly-once | 2 / 0 | exercice-flink-panne | 6 |
| Réplication HDFS en production | 3 | doc-apache | 7 |
| Réplication HDFS dans l'exercice | 1 | exercice-hdfs | 7 |

## Outline

| # | Archetype | Title — a claim, not a topic | Bottom line |
| :-- | :--- | :--- | :--- |
| 1 | A-10 Architecture View | La restauration juge la sauvegarde | Deux mesures comptent : les données qu'on accepte de perdre (RPO) et le temps pour repartir (RTO). |
| 2 | A-05 Animated Trajectory | Incrémentale légère, différentielle vite restaurée | Après une panne le jeudi, l'incrémentale demande 4 copies à recharger, la différentielle seulement 2. |
| 3 | A-10 Architecture View | Deux briques gardent les données brutes | Kafka et la zone bronze de HDFS gardent les données d'origine ; tout le reste se recalcule à partir d'elles. |
| 4 | A-04 Two-Column Ledger | Kafka réplique, il ne sauvegarde pas | La réplication survit à la panne d'un broker, mais une suppression par erreur est répliquée partout. |
| 5 | A-08 Process / Flow | Flink se sauvegarde lui-même, en continu | Un checkpoint enregistre l'état et les offsets Kafka : complet avec HashMap, incrémental avec RocksDB. |
| 6 | A-09 Timeline with a Gate | 35 s d'arrêt, zéro donnée perdue | Le job est reparti du checkpoint n° 22 : aucune donnée perdue, 35 s d'interruption. |
| 7 | A-10 Architecture View | Bronze sauvegardé, silver et gold recalculés | HDFS protège bronze par réplication et snapshots ; silver et gold se reconstruisent en relançant Spark. |
| 8 | A-14 Verdict / Close | Protégez d'abord ce qui est irremplaçable | Avant de choisir totale, incrémentale ou différentielle, vérifiez si la donnée peut être reconstruite à partir d'une autre. |
