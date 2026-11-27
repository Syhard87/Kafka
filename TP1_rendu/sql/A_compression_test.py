#!/usr/bin/env python3
"""Optimisation du stockage Kafka : effet du batching (linger.ms) et de la compression.

On envoie les MÊMES 30 000 commandes (générées comme generator.py, graine fixe) dans 4 topics
d'une partition, avec 4 réglages producteur, puis on mesure la taille réelle du segment sur le broker.

Usage : python A_compression_test.py   (cluster du kit démarré, depuis la machine hôte)
"""
import json
import random
import subprocess
from datetime import datetime, timedelta

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

BOOT = "localhost:19092,localhost:29092,localhost:39092"
N = 30_000
PRODUCTS = ["clavier", "souris", "ecran", "casque", "webcam", "disque", "cable", "chargeur"]
COUNTRIES = ["fr", "be", "ch", "de", "es", "it", "tn", "ma"]

rng = random.Random(1)
t0 = datetime(2026, 11, 27, 9, 0, 0)
messages = []
for i in range(N):
    ts = t0 + timedelta(milliseconds=20 * i)
    evt = {"order_id": 1795700000000 + i, "customer_id": f"C{rng.randint(1, 1000):06d}",
           "product": rng.choice(PRODUCTS), "amount": round(rng.uniform(5, 500), 2),
           "country": rng.choice(COUNTRIES),
           "ts": ts.strftime("%Y-%m-%d %H:%M:%S.") + f"{ts.microsecond // 1000:03d}"}
    messages.append((evt["customer_id"], json.dumps(evt)))

VARIANTES = [  # (topic, linger.ms, compression)
    ("test_l0_none", 0, "none"),          # 1 message par requête : pire cas
    ("test_l5_none", 5, "none"),          # réglage du générateur du kit
    ("test_l50_lz4", 50, "lz4"),          # batching + compression rapide
    ("test_l50_zstd", 50, "zstd"),        # batching + meilleure compression
]

admin = AdminClient({"bootstrap.servers": BOOT})
admin.delete_topics([v[0] for v in VARIANTES])
import time; time.sleep(3)
for f in admin.create_topics([NewTopic(v[0], 1, 3) for v in VARIANTES]).values():
    f.result()

brut = sum(len(v.encode()) + len(k.encode()) for k, v in messages)
print(f"{N} messages JSON, {brut / N:.0f} octets utiles en moyenne (clé + valeur)\n")
print(f"{'topic':<15} {'linger.ms':>9} {'compression':>11} {'batches':>8} {'octets/msg':>10} {'taille':>9} {'gain':>6}")
ref = None
for topic, linger, comp in VARIANTES:
    p = Producer({"bootstrap.servers": BOOT, "acks": "all", "enable.idempotence": True,
                  "linger.ms": linger, "compression.type": comp, "batch.size": 131072})
    for i, (k, v) in enumerate(messages):
        p.produce(topic, key=k, value=v)
        p.poll(0)
        if i % 50 == 49:          # ~2 500 msg/s : on laisse le temps au linger de jouer
            time.sleep(0.02)
    p.flush()
    seg = f"/tmp/kraft-combined-logs/{topic}-0/00000000000000000000.log"
    taille = int(subprocess.run(["docker", "exec", "kafka1", "stat", "-c", "%s", seg],
                                capture_output=True, text=True).stdout)
    dump = subprocess.run(["docker", "exec", "kafka1", "/opt/kafka/bin/kafka-dump-log.sh", "--files", seg],
                          capture_output=True, text=True).stdout
    batches = dump.count("baseOffset:")
    ref = ref or taille
    print(f"{topic:<15} {linger:>9} {comp:>11} {batches:>8} {taille / N:>10.1f} {taille / 1e6:>7.2f} Mo {ref / taille:>5.1f}x")
