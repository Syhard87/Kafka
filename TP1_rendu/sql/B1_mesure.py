#!/usr/bin/env python3
"""B1 : tableau comparatif des 3 watermarks (nombre de commandes comptées et délai d'émission).

- Vérité terrain : on relit TOUT le topic "commandes" et on compte, par minute d'événement (ts),
  les commandes de montant > 0 (même règle que le job Flink).
- Résultats Flink : topics ca_wm5, ca_wm30, ca_wm60. Ces topics sont en LogAppendTime :
  l'horodatage Kafka de chaque résultat est l'heure à laquelle Flink l'a écrit.
  Délai d'émission = horodatage Kafka - fin de la fenêtre (w_end).

Usage : python B1_mesure.py   (depuis la machine hôte, cluster démarré)
"""
import json
from collections import defaultdict
from datetime import datetime, timezone

from confluent_kafka import Consumer, TopicPartition

BOOTSTRAP = "localhost:19092,localhost:29092,localhost:39092"


def lire_topic(topic):
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"mesure-b1-{datetime.now().timestamp()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    meta = c.list_topics(topic, timeout=10)
    parts = [TopicPartition(topic, p, 0) for p in meta.topics[topic].partitions]
    fins = {p.partition: c.get_watermark_offsets(p)[1] for p in parts}
    c.assign(parts)
    msgs, lus = [], defaultdict(int)
    while any(lus[p] < fins[p] for p in fins):
        m = c.poll(2.0)
        if m is None or m.error():
            continue
        lus[m.partition()] = m.offset() + 1
        msgs.append((m.timestamp()[1], m.value()))
    c.close()
    return msgs


def minute(ts_txt):
    return ts_txt[:16]                     # "2026-11-26 20:51"


def main():
    verite = defaultdict(int)
    for _, v in lire_topic("commandes"):
        try:
            e = json.loads(v)
        except ValueError:
            continue
        if e.get("ts") and e.get("amount", 0) > 0:
            verite[minute(e["ts"])] += 1

    resultats = {}
    for variante in ("ca_wm5", "ca_wm30", "ca_wm60"):
        for ts_kafka, v in lire_topic(variante):
            r = json.loads(v)
            w_end = datetime.strptime(r["w_end"][:19], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            emis = datetime.fromtimestamp(ts_kafka / 1000, tz=timezone.utc)
            resultats[(variante, minute(r["w_start"]))] = (r["nb"], r["total"], (emis - w_end).total_seconds())

    minutes = sorted({m for (_, m) in resultats})
    print(f"{'minute':<17} {'réel':>5} | {'wm 5 s':>16} {'délai':>6} | {'wm 30 s':>16} {'délai':>6} | "
          f"{'wm 60 s':>16} {'délai':>6}")
    for m in minutes:
        ligne = f"{m:<17} {verite[m]:>5} |"
        for variante in ("ca_wm5", "ca_wm30", "ca_wm60"):
            if (variante, m) in resultats:
                nb, _, delai = resultats[(variante, m)]
                perte = 100 * (verite[m] - nb) / verite[m] if verite[m] else 0
                ligne += f" {nb:>6} (-{perte:4.1f} %) {delai:>5.1f}s |"
            else:
                ligne += f" {'non émis':>16} {'':>6} |"
        print(ligne.rstrip("|"))
    print("\nréel = commandes de montant > 0 réellement produites pour cette minute (relecture du topic) ;"
          "\n(-x %) = commandes perdues car arrivées après le watermark ; délai = émission - fin de minute.")


if __name__ == "__main__":
    main()
