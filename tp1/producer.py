#!/usr/bin/env python3
"""TP1 partie C : producteur Python du topic commandes.

Envoie 1000 commandes pour 10 clients (C0000 à C0009), avec la clé = customer_id.
acks=all + idempotence : chaque message est écrit sur toutes les répliques synchronisées,
sans doublon en cas de nouvel envoi automatique.

    python producer.py
"""
import json

from confluent_kafka import Producer

p = Producer({"bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
              "acks": "all", "enable.idempotence": True})

stats = {"ok": 0, "err": 0}


def rapport(err, msg):
    if err:
        stats["err"] += 1
        print("Erreur :", err)
    else:
        stats["ok"] += 1


for i in range(1000):
    evt = {"order_id": i, "customer_id": f"C{i % 10:04d}", "amount": 10 + i % 50}
    p.produce("commandes", key=evt["customer_id"], value=json.dumps(evt), callback=rapport)
    p.poll(0)
p.flush()

print(f"Terminé : 1000 commandes envoyées, {stats['ok']} confirmées, {stats['err']} erreurs.")
