"""
Bridges Kafka -> HDFS. Reads `student-health-raw`, buffers rows per
partition, and flushes each buffer to a CSV file under
/data/raw/student_depression/ in HDFS once it hits a size or time threshold.

Design notes (why it's built this way):

  * Never one file per message. The HDFS NameNode keeps all file metadata
    in memory; thousands of tiny files hurt it far more than a few large
    ones (the "small files problem"). We flush on whichever comes first:
    FLUSH_MAX_RECORDS or FLUSH_MAX_SECONDS.

  * Kafka offsets are committed only *after* the HDFS write succeeds
    (enable.auto.commit is off; commits are manual). If this process dies
    mid-write, the un-committed records are simply re-read on restart --
    at-least-once delivery.

  * Filenames are deterministic: part{partition}_offsets_{start}-{end}.csv.
    A replayed batch after a crash overwrites the exact same HDFS path
    instead of creating a duplicate file. At-least-once delivery + an
    idempotent (overwriting) write together give effectively-exactly-once
    results downstream.
"""
import csv
import io
import signal
import sys
import time

from confluent_kafka import Consumer, TopicPartition
from hdfs import InsecureClient

TOPIC = "student-health-raw"
BOOTSTRAP_SERVERS = "localhost:9092"
WEBHDFS_URL = "http://localhost:9870"
HDFS_DIR = "/data/raw/student_depression"

FLUSH_MAX_RECORDS = 2000
FLUSH_MAX_SECONDS = 15

FIELDNAMES = [
    "id", "Gender", "Age", "City", "Profession", "Academic Pressure",
    "Work Pressure", "CGPA", "Study Satisfaction", "Job Satisfaction",
    "Sleep Duration", "Dietary Habits", "Degree",
    "Have you ever had suicidal thoughts ?", "Work/Study Hours",
    "Financial Stress", "Family History of Mental Illness", "Depression",
    "batch_id", "ingest_ts",
]

running = True


def handle_shutdown(signum, frame):
    global running
    running = False


signal.signal(signal.SIGINT, handle_shutdown)
signal.signal(signal.SIGTERM, handle_shutdown)


class PartitionBuffer:
    def __init__(self):
        self.records = []       # list of dict rows
        self.start_offset = None
        self.end_offset = None
        self.last_flush = time.time()

    def add(self, offset, row):
        if self.start_offset is None:
            self.start_offset = offset
        self.end_offset = offset
        self.records.append(row)

    def should_flush(self):
        if not self.records:
            return False
        if len(self.records) >= FLUSH_MAX_RECORDS:
            return True
        if time.time() - self.last_flush >= FLUSH_MAX_SECONDS:
            return True
        return False

    def reset(self):
        self.records = []
        self.start_offset = None
        self.end_offset = None
        self.last_flush = time.time()


def write_to_hdfs(hdfs_client, partition, buf: PartitionBuffer):
    filename = f"part{partition}_offsets_{buf.start_offset}-{buf.end_offset}.csv"
    path = f"{HDFS_DIR}/{filename}"

    out = io.StringIO()
    writer = csv.DictWriter(out, fieldnames=FIELDNAMES)
    writer.writeheader()
    for row in buf.records:
        writer.writerow({k: row.get(k, "") for k in FIELDNAMES})

    # overwrite=True makes a replay after a crash idempotent: same offsets ->
    # same filename -> same content, not a duplicate.
    hdfs_client.write(path, data=out.getvalue(), encoding="utf-8", overwrite=True)
    print(f"[consumer] wrote {len(buf.records)} rows -> hdfs://{path}")


def main():
    hdfs_client = InsecureClient(WEBHDFS_URL, user="root")
    hdfs_client.makedirs(HDFS_DIR)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "hdfs-writer",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe([TOPIC])

    buffers = {}  # partition -> PartitionBuffer

    print(f"[consumer] listening on '{TOPIC}', flushing every {FLUSH_MAX_RECORDS} records or {FLUSH_MAX_SECONDS}s")

    try:
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is not None and not msg.error():
                partition = msg.partition()
                buf = buffers.setdefault(partition, PartitionBuffer())
                row = _parse_row(msg.value())
                buf.add(msg.offset(), row)
            elif msg is not None and msg.error():
                print(f"[consumer] error: {msg.error()}", file=sys.stderr)

            for partition, buf in buffers.items():
                if buf.should_flush():
                    write_to_hdfs(hdfs_client, partition, buf)
                    consumer.commit(offsets=[TopicPartition(TOPIC, partition, buf.end_offset + 1)])
                    buf.reset()
    finally:
        print("[consumer] shutting down, flushing remaining buffers...")
        for partition, buf in buffers.items():
            if buf.records:
                write_to_hdfs(hdfs_client, partition, buf)
                consumer.commit(offsets=[TopicPartition(TOPIC, partition, buf.end_offset + 1)])
        consumer.close()


def _parse_row(raw_bytes):
    import json
    return json.loads(raw_bytes.decode("utf-8"))


if __name__ == "__main__":
    main()
