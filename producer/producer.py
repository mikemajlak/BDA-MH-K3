"""
Streams the student-depression CSV into Kafka topic `student-health-raw`,
batch by batch, so the rest of the pipeline sees an actual stream rather than
one bulk file.

Each message is one student row as JSON, keyed by `id` (so all messages for
a given student land on the same partition -> per-key ordering). Every row
gets a `batch_id` and `ingest_ts` so the HDFS/consumer side can reason about
when data arrived.

Usage:
    python producer.py [--csv PATH] [--topic NAME] [--bootstrap-servers HOST:PORT]
                        [--batch-size N] [--sleep-seconds S] [--replicate N]

--replicate N: the raw dataset is only ~3MB, i.e. a single 128MB HDFS block
and a single mapper -- not enough to demonstrate real parallelism. Passing
--replicate 100 replays the dataset 100x with new synthetic ids
(original_id * 100000 + replica_index) so the pipeline can be re-run at a
scale where Hadoop actually splits the input across multiple mappers. This
is clearly synthetic and should be reported as such, never as more data.
"""
import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic


def ensure_topic(bootstrap_servers: str, topic: str, partitions: int = 3):
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        return
    fs = admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])
    for t, f in fs.items():
        try:
            f.result()
            print(f"[producer] created topic '{t}' with {partitions} partitions")
        except Exception as e:
            print(f"[producer] topic creation for '{t}' failed (may already exist): {e}")


def delivery_report(err, msg):
    if err is not None:
        print(f"[producer] delivery failed for key={msg.key()}: {err}", file=sys.stderr)


def stream_rows(csv_path: str, replicate: int):
    # DictReader.fieldnames is lazily set on its first __next__ call, which
    # then ALSO reads the following row in that same call -- re-seeking to 0
    # and calling next(reader) again does not cleanly "skip the header" a
    # second time, it desyncs the reader. Load once into memory instead
    # (dataset is a few MB) and replay from the list per replica.
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    for replica_idx in range(replicate):
        for row in rows:
            if replicate > 1:
                row = dict(row)
                row["id"] = str(int(row["id"]) * 100000 + replica_idx)
            yield row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default="dataset/Student Depression Dataset.csv")
    ap.add_argument("--topic", default="student-health-raw")
    ap.add_argument("--bootstrap-servers", default="localhost:9092")
    ap.add_argument("--batch-size", type=int, default=500, help="rows per batch before a sleep pause")
    ap.add_argument("--sleep-seconds", type=float, default=1.0, help="pause between batches, to simulate streaming")
    ap.add_argument("--replicate", type=int, default=1, help="replay the dataset N times with synthetic ids (scale-up mode)")
    args = ap.parse_args()

    ensure_topic(args.bootstrap_servers, args.topic)

    producer = Producer({
        "bootstrap.servers": args.bootstrap_servers,
        "client.id": "student-health-producer",
        "acks": "all",
        "linger.ms": 20,
    })

    sent = 0
    batch_id = 0
    for row in stream_rows(args.csv, args.replicate):
        row["batch_id"] = batch_id
        row["ingest_ts"] = datetime.now(timezone.utc).isoformat()
        payload = json.dumps(row).encode("utf-8")
        producer.produce(
            args.topic,
            key=str(row["id"]).encode("utf-8"),
            value=payload,
            callback=delivery_report,
        )
        producer.poll(0)
        sent += 1

        if sent % args.batch_size == 0:
            batch_id += 1
            producer.flush()
            print(f"[producer] sent {sent} rows (batch {batch_id})")
            time.sleep(args.sleep_seconds)

    producer.flush()
    print(f"[producer] done. total rows sent: {sent}")


if __name__ == "__main__":
    main()
