#!/usr/bin/env python3
"""TP6 : lit le topic alertes et affiche chaque alerte lisiblement.

    python alertes_consumer.py                 # nouvelles alertes uniquement
    python alertes_consumer.py --depuis-debut  # relit tout le topic

Le pipeline Flink écrit en at-least-once : après une reprise sur panne, une même alerte
peut être réémise. Le consommateur les dédoublonne sur (client, début de fenêtre, motif).
"""
import argparse
import json
import time
from collections import Counter

from confluent_kafka import Consumer

BOOTSTRAP = "localhost:19092,localhost:29092,localhost:39092"
COULEURS = {"HAUTE": "\033[91m", "MOYENNE": "\033[93m"}
FIN = "\033[0m"


def parse_args():
    p = argparse.ArgumentParser(description="Affichage des alertes du pipeline Flink (TP6)")
    p.add_argument("--depuis-debut", action="store_true", help="relire le topic depuis le début")
    p.add_argument("--sans-couleur", action="store_true", help="pas de couleurs (pour un fichier)")
    p.add_argument("--arret-inactif", type=float, default=0,
                   help="s'arrête après N secondes sans alerte (0 = jamais)")
    return p.parse_args()


def montant(x):
    return f"{x:,.2f} €".replace(",", " ").replace(".", ",")


def main():
    args = parse_args()
    c = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        # Groupe unique en mode --depuis-debut, pour ne pas dépendre d'offsets déjà enregistrés
        "group.id": f"alertes-console-{int(time.time())}" if args.depuis_debut else "alertes-console",
        "auto.offset.reset": "earliest" if args.depuis_debut else "latest",
    })
    c.subscribe(["alertes"])

    vues = set()
    stats = Counter()
    doublons = 0
    dernier = time.monotonic()
    print("En attente d'alertes (Ctrl+C pour arrêter)...")
    try:
        while True:
            m = c.poll(1.0)
            if m is None:
                if args.arret_inactif and time.monotonic() - dernier > args.arret_inactif:
                    break
                continue
            if m.error():
                continue
            dernier = time.monotonic()
            a = json.loads(m.value())
            cle = (a["customer_id"], a["w_start"], a["motif"])
            if cle in vues:
                doublons += 1
                continue
            vues.add(cle)
            stats[a.get("gravite", "MOYENNE")] += 1

            gravite = a.get("gravite", "MOYENNE")
            debut, fin = a["w_start"][11:19], a["w_end"][11:19]
            ligne = (f"[{debut} → {fin}] {gravite:<7} client {a['customer_id']} : "
                     f"{a['nb']} commande(s), {montant(a['total'])} | {a['motif']}")
            if not args.sans_couleur:
                ligne = COULEURS.get(gravite, "") + ligne + FIN
            print(ligne, flush=True)
    except KeyboardInterrupt:
        pass
    finally:
        c.close()
        total = sum(stats.values())
        print(f"\n{total} alertes affichées (HAUTE : {stats['HAUTE']}, MOYENNE : {stats['MOYENNE']}), "
              f"{doublons} doublons ignorés.")


if __name__ == "__main__":
    main()
