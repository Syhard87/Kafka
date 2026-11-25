#!/usr/bin/env python3
"""Générateur d'événements d'expéditions pour la Control Tower (exercice intégré, 3iL).

Chaque expédition émet, dans l'ordre :
    CREE -> EN_TRANSIT -> EN_DOUANE (seulement si le pays d'origine est hors UE) -> LIVRE
Environ 10 % des expéditions sont « bloquées » : leurs événements s'arrêtent avant LIVRE.

Le temps est compressé : une expédition dure quelques minutes au lieu de quelques jours.
Deux événements successifs d'une même expédition sont espacés de 8 à 45 secondes, donc
toujours moins de 90 secondes : une expédition bloquée est la seule dont l'activité s'arrête
sans LIVRE.

Exemples :
  python generator_expeditions.py --nb 100 --topic expeditions
  python generator_expeditions.py --nb 60 --no-key               # sans clé (comparaison)
  python generator_expeditions.py --dry-run                      # affiche des événements, sans Kafka

Chaque message (clé = id_expedition, sauf avec --no-key) :
  {"id_expedition": "EXP-4821-0007", "statut": "EN_TRANSIT", "ts_evenement": "2026-11-23 10:15:03.123",
   "transporteur": "NordFret", "fournisseur": "Aciéries du Rhin", "pays_origine": "DE", "pays_dest": "FR",
   "delai_promis_j": 4, "poids_kg": 812.5, "contact_email": null}
Le champ contact_email n'est renseigné que sur l'événement CREE.
"""
import argparse
import json
import random
import sys
import time
from datetime import datetime

PAYS_UE = {"FR", "DE", "IT", "ES", "PL", "BE", "NL"}

# Fournisseurs fictifs : pays d'origine, transporteurs possibles, délai promis (jours)
FOURNISSEURS = [
    ("Aciéries du Rhin", "DE", ["NordFret", "TransEurope Express"], (3, 5)),
    ("Lumitech Milano", "IT", ["TransEurope Express", "MedCargo"], (4, 6)),
    ("Polska Komponenty", "PL", ["NordFret", "RapidRoute"], (4, 6)),
    ("Iberia Plast", "ES", ["MedCargo", "RapidRoute"], (3, 6)),
    ("Lyon Électronique", "FR", ["RapidRoute", "TransEurope Express"], (2, 3)),
    ("Atlas Textiles", "MA", ["MedCargo"], (8, 12)),
    ("Anatolia Metal", "TR", ["MedCargo", "TransEurope Express"], (8, 12)),
    ("Shenzhen Microparts", "CN", ["AsiaLink Logistics"], (25, 35)),
    ("Pune Precision", "IN", ["AsiaLink Logistics"], (25, 35)),
    ("Ohio Bearings", "US", ["NordFret", "AsiaLink Logistics"], (12, 18)),
]


def parse_args():
    p = argparse.ArgumentParser(description="Générateur d'événements d'expéditions (Control Tower)")
    p.add_argument("--bootstrap", default="localhost:19092,localhost:29092,localhost:39092",
                   help="adresses des brokers vues depuis votre machine")
    p.add_argument("--topic", default="expeditions", help="topic de destination (défaut : expeditions)")
    p.add_argument("--nb", type=int, default=100, help="nombre d'expéditions (défaut : 100)")
    p.add_argument("--taux-blocage", type=float, default=0.10,
                   help="part des expéditions jamais livrées, entre 0 et 1 (défaut : 0.10)")
    p.add_argument("--no-key", action="store_true", help="envoyer les messages sans clé")
    p.add_argument("--seed", type=int, default=None, help="graine aléatoire, pour rejouer la même suite")
    p.add_argument("--dry-run", action="store_true", help="affiche 12 événements sans se connecter à Kafka")
    a = p.parse_args()
    if a.nb < 1:
        p.error("--nb doit être au moins 1")
    if not 0 <= a.taux_blocage <= 1:
        p.error("--taux-blocage doit être compris entre 0 et 1")
    return a


def plan_expeditions(args, rng):
    """Calcule, pour chaque expédition, la liste de ses événements et leur instant d'émission
    (en secondes depuis le début du flux)."""
    lot = rng.randint(1000, 9999)                       # identifie ce lancement du générateur
    etalement = max(30.0, min(args.nb * 1.2, 180.0))    # les expéditions démarrent sur 30 à 180 s
    nb_bloquees = round(args.nb * args.taux_blocage)
    bloquees = set(rng.sample(range(args.nb), nb_bloquees))
    plan = []
    for i in range(args.nb):
        fournisseur, pays, transporteurs, (dmin, dmax) = rng.choice(FOURNISSEURS)
        statuts = ["CREE", "EN_TRANSIT"] + ([] if pays in PAYS_UE else ["EN_DOUANE"]) + ["LIVRE"]
        if i in bloquees:
            statuts = statuts[:rng.randint(1, len(statuts) - 1)]   # s'arrête avant LIVRE
        base = {
            "id_expedition": f"EXP-{lot}-{i + 1:04d}",
            "transporteur": rng.choice(transporteurs),
            "fournisseur": fournisseur,
            "pays_origine": pays,
            "pays_dest": "FR",
            "delai_promis_j": rng.randint(dmin, dmax),
            "poids_kg": round(rng.uniform(20, 2500), 1),
        }
        contact = "logistique@" + fournisseur.lower().replace(" ", "-").replace("é", "e") + ".example"
        t = rng.uniform(0, etalement)
        for s in statuts:
            plan.append((t, i, s, base, contact if s == "CREE" else None))
            t += rng.uniform(8, 45)
    plan.sort(key=lambda e: e[0])
    return lot, plan, len(bloquees)


def evenement(base, statut, contact):
    now = datetime.now()
    evt = dict(base)
    evt["statut"] = statut
    evt["ts_evenement"] = now.strftime("%Y-%m-%d %H:%M:%S.") + f"{now.microsecond // 1000:03d}"
    evt["contact_email"] = contact
    ordre = ["id_expedition", "statut", "ts_evenement", "transporteur", "fournisseur", "pays_origine",
             "pays_dest", "delai_promis_j", "poids_kg", "contact_email"]
    return {k: evt[k] for k in ordre}


def main():
    args = parse_args()
    rng = random.Random(args.seed)
    lot, plan, nb_bloquees = plan_expeditions(args, rng)

    if args.dry_run:
        for _, _, statut, base, contact in plan[:12]:
            e = evenement(base, statut, contact)
            print(f"clé={'(aucune)' if args.no_key else e['id_expedition']}  {json.dumps(e, ensure_ascii=False)}")
        return 0

    try:
        from confluent_kafka import Producer, KafkaException
    except ImportError:
        print("ERREUR : la bibliothèque confluent-kafka n'est pas installée (pip install confluent-kafka).")
        return 1

    stats = {"ok": 0, "err": 0, "last_err": None}

    def on_delivery(err, msg):
        if err:
            stats["err"] += 1
            stats["last_err"] = str(err)
        else:
            stats["ok"] += 1

    producer = Producer({"bootstrap.servers": args.bootstrap, "client.id": "control-tower-generator",
                         "acks": "all", "enable.idempotence": True, "linger.ms": 5})
    try:
        meta = producer.list_topics(timeout=10)
    except KafkaException as e:
        print(f"ERREUR : impossible de joindre Kafka sur {args.bootstrap} ({e})")
        return 1
    if args.topic not in meta.topics:
        print(f"ERREUR : le topic '{args.topic}' n'existe pas (création automatique désactivée).")
        return 1

    duree = plan[-1][0]
    print(f"Lot {lot} : {args.nb} expéditions, {len(plan)} événements vers '{args.topic}' "
          f"en {duree / 60:.1f} min environ, {'sans clé' if args.no_key else 'clé = id_expedition'}.")
    debut = time.monotonic()
    prochain_point = debut + 30
    envoyes = 0
    try:
        for t, _, statut, base, contact in plan:
            attente = debut + t - time.monotonic()
            if attente > 0:
                time.sleep(attente)
            e = evenement(base, statut, contact)
            while True:
                try:
                    producer.produce(args.topic, key=None if args.no_key else e["id_expedition"],
                                     value=json.dumps(e, ensure_ascii=False), callback=on_delivery)
                    break
                except BufferError:
                    producer.poll(0.1)
            producer.poll(0)
            envoyes += 1
            if time.monotonic() >= prochain_point:
                print(f"[{time.monotonic() - debut:5.0f} s] envoyés={envoyes}/{len(plan)}  "
                      f"confirmés={stats['ok']}  erreurs={stats['err']}")
                if stats["last_err"]:
                    print(f"        dernière erreur : {stats['last_err']}")
                prochain_point += 30
    except KeyboardInterrupt:
        print("\nArrêt demandé...")
    finally:
        reste = producer.flush(15)
    print(f"Terminé : {envoyes} événements envoyés, {stats['ok']} confirmés, {stats['err']} erreurs"
          + (f", {reste} non confirmés" if reste else "") + ".")
    print(f"Vérité terrain : {nb_bloquees} expéditions sur {args.nb} ne recevront jamais LIVRE (lot {lot}).")
    return 0 if stats["err"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
