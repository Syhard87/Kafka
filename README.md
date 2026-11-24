# TP Kafka / Flink

Module « Traitement de flux en temps réel (Kafka, Flink) », 3iL, 2026-2027.

## Le compte rendu des TP (TP0 à TP6)

| Fichier | Contenu |
|---|---|
| `compte-rendu/Compte_rendu_TP_Kafka_Flink.docx` | Compte rendu Word, 47 pages : commandes, sorties obtenues, captures, réponses à toutes les questions (Q1, Q2…), exercices 2 à 5 |
| `compte-rendu/Compte_rendu_TP_Kafka_Flink.pdf` | Le même en PDF, sommaire à jour |
| `compte-rendu/captures/` | Les 14 captures d'écran (Kafka UI, Flink, Prometheus, Grafana) |
| `tp1/producer.py`, `tp1/consumer.py` | TP1 C (et TP5 B) : producteur ; consommateur avec commit après traitement, `--panne 500`, `--idempotent`, `--lent 0.01` |
| `tp3/03_enrichissement.sql`, `tp3/verif_ca_minute.py` | TP3 C : job Kafka vers Kafka ; TP3 Q7 : recalcul du chiffre d'affaires par minute |
| `tp4/retards.sql`, `tp4/etat_ttl.sql`, `tp4/ca_minute_exactly_once.sql` | TP4 B, C, E : watermarks 0 / 5 / 60 s, TTL, sink exactly-once |
| `tp5/skew_apres.sql`, `tp5/skew_salage.sql` | TP5 C : mini-batch + deux phases ; salage de clé |
| `tp6/pipeline.sql`, `tp6/alertes_consumer.py` | TP6 : pipeline complet en un seul job ; affichage des alertes avec dédoublonnage |
| `TP_Kafka_Flink.pdf`, `Kit_TP_Kafka_Flink.zip` | Énoncé et kit fournis par l'enseignant |

À compléter avant de rendre : la page de garde du compte rendu (noms du binôme, groupe, enseignant, date).
Dans Word, accepter la mise à jour des champs à l'ouverture (sommaire), ou faire clic droit sur le sommaire,
« Mettre à jour les champs ».

Les sorties et captures du compte rendu ont été obtenues en exécutant réellement les TP avec le kit,
sur une machine Linux (Docker, 4 processeurs virtuels, 15 Go de mémoire). Les mesures de performance du TP5
dépendent de la machine.

Lancer les fichiers SQL : copier le fichier dans `kit/sql/` (monté sur `/opt/sql` dans le JobManager), puis
`docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql -f /opt/sql/<fichier>.sql`.
Lancer les scripts Python depuis le dossier du kit, environnement virtuel activé.

## Préparation du poste (guide d'installation)

Le document `Guide_Installation_TP_Docker (1).docx` est le **guide d'installation de l'environnement**,
à suivre **sur ton propre PC, avant le premier TP** (compter 1 h à 1 h 30 et environ 3 Go à télécharger).

Pour ce guide, le rendu, ce sont **2 captures d'écran de ton poste** à envoyer à l'enseignant :

| Capture | Ce qu'on doit voir | Adresse |
|---|---|---|
| n°1 | Kafka UI : cluster **tp3il** avec **3 brokers** | http://localhost:8080 |
| n°2 | Interface Flink : **1 TaskManager** et **8 slots** | http://localhost:8081 |

Si ton PC ne peut pas faire tourner Docker (moins de 8 Go de mémoire, pas de droits administrateur),
préviens l'enseignant **avant le premier cours** : tu travailleras en binôme.

Le kit `Kit_TP_Kafka_Flink.zip` est à la racine de ce dépôt : le dézipper dans `C:\tp-kafka`.

### Méthode rapide sous Windows : 2 scripts

Les scripts suivent le guide étape par étape, vérifient chaque résultat et te disent quoi faire en cas
de problème. `-ExecutionPolicy Bypass` n'autorise le script que pour ce lancement-là.

#### Script 1 : WSL 2 et Docker Desktop (PowerShell en administrateur)

Clic droit sur PowerShell, « Exécuter en tant qu'administrateur », puis :

```powershell
powershell -ExecutionPolicy Bypass -File C:\Dev\Kafka\scripts\1-installer-wsl-docker.ps1
```

Il vérifie la mémoire, le disque et la virtualisation, installe ou met à jour WSL 2, écrit le fichier
`.wslconfig` (5 Go pour Docker sur un PC de 8 Go, 6 Go sinon, sans le piège du `.txt`) et propose
d'installer Docker Desktop avec winget.

Ensuite : **redémarre** si le script le demande, lance Docker Desktop (« Skip » pour le compte) et
attends que la baleine soit immobile.

#### Script 2 : kit, Python, images et tests (PowerShell normal)

Dézippe le kit dans `C:\tp-kafka` (chemin sans espaces ni accents), puis :

```powershell
powershell -ExecutionPolicy Bypass -File C:\Dev\Kafka\scripts\2-tester-kit.ps1 -Kit C:\tp-kafka
```

Il enchaîne les étapes 3 à 5 du guide :

1. trouve Python 3.12 ou 3.11, crée `.venv` dans le kit et installe `confluent-kafka` ;
2. lance `generator.py --dry-run` ;
3. télécharge les images et construit l'image Flink (10 à 30 min la première fois) ;
4. démarre les 3 brokers et Kafka UI, vérifie les 3 brokers, ouvre la page et **attend la capture n°1** ;
5. crée le topic `test-install`, envoie 50 messages depuis Python, vérifie « 0 erreurs », supprime le topic ;
6. démarre Flink, vérifie 1 TaskManager et 8 slots, ouvre la page et **attend la capture n°2** ;
7. vérifie que le client SQL de Flink voit la table `commandes` ;
8. affiche un bilan et propose d'arrêter les conteneurs.

Les captures sont à enregistrer dans le dossier `captures/` de ce repo (le script indique le chemin exact).
Pour relancer les tests sans tout retélécharger : ajoute `-SansTelechargement`.

### Checklist à la main (si tu préfères ne pas utiliser les scripts)

- [ ] Virtualisation activée (Gestionnaire des tâches, Performances, Processeur)
- [ ] `wsl --install`, redémarrage, `wsl --status` affiche 2
- [ ] Docker Desktop installé et lancé, option « Use WSL 2 instead of Hyper-V »
- [ ] `C:\Users\<toi>\.wslconfig` contient `[wsl2]`, `memory=6GB` (5GB si 8 Go), `processors=4` ; puis `wsl --shutdown`
- [ ] `docker run hello-world` affiche « Hello from Docker! »
- [ ] Kit dézippé dans `C:\tp-kafka`, terminal ouvert dans le dossier qui contient `docker-compose.yml`
- [ ] Python 3.12 installé (« Add python.exe to PATH » coché)
- [ ] `py -3.12 -m venv .venv`, `.venv\Scripts\activate`, `pip install -r requirements.txt`
- [ ] `python generator.py --dry-run` affiche 5 commandes JSON
- [ ] `docker compose --profile flink --profile monitoring pull --ignore-buildable` puis `docker compose --profile flink build`
- [ ] `docker compose up -d kafka1 kafka2 kafka3 kafka-ui` : http://localhost:8080 montre tp3il avec 3 brokers, **capture n°1**
- [ ] Topic `test-install` : le générateur finit par « 50 confirmés, 0 erreurs »
- [ ] `docker compose --profile flink up -d` : http://localhost:8081 montre 1 TaskManager et 8 slots, **capture n°2**
- [ ] Client SQL : `SHOW TABLES;` affiche `commandes`
- [ ] Captures envoyées à l'enseignant

## Pendant les TP

| Action | Commande (depuis le dossier du kit) |
|---|---|
| Activer Python (à chaque nouveau terminal) | `.venv\Scripts\activate` |
| Démarrer Kafka (tous les TP) | `docker compose up -d kafka1 kafka2 kafka3 kafka-ui` |
| Démarrer Flink (TP3 à TP6) | `docker compose --profile flink up -d` |
| Démarrer la supervision (TP5) | `docker compose --profile monitoring up -d` |
| Client SQL de Flink | `docker compose exec jobmanager ./bin/sql-client.sh -i /opt/sql/init.sql` |
| Lire les journaux | `docker compose logs -f kafka1` |
| Tout arrêter (données conservées) | `docker compose --profile flink --profile monitoring stop` |

Kafka depuis Python : `localhost:19092`, `localhost:29092`, `localhost:39092`.
Kafka depuis un conteneur : `kafka1:9092`, `kafka2:9092`, `kafka3:9092`.
Grafana : http://localhost:3000 (admin / admin), Prometheus : http://localhost:9090.

Sur un PC de 8 Go : ne pas lancer la supervision (TP5, partie D) en même temps que Flink.
Pour tout problème, voir la section « Dépannage » du guide Word.

## Contenu du repo

```
compte-rendu/                           compte rendu Word + PDF, captures d'écran
tp1/ tp3/ tp4/ tp5/ tp6/                scripts Python et requêtes SQL des TP
TP_Kafka_Flink.pdf                      énoncé des TP
Kit_TP_Kafka_Flink.zip                  kit fourni par l'enseignant
Guide_Installation_TP_Docker (1).docx   guide d'installation fourni par l'enseignant
scripts/1-installer-wsl-docker.ps1      guide, étape 1 : WSL 2, .wslconfig, Docker Desktop (admin)
scripts/2-tester-kit.ps1                guide, étapes 2 à 5 : Python, images, tests, captures du poste
captures/                               créé par le script 2 pour y ranger les 2 captures du guide
```
