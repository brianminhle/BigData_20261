"""Durable snapshot outbox. Run one writer per bucket/prefix."""

from dataclasses import asdict
import hashlib
import io
import json


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


class SnapshotStore:
    def __init__(self, client, bucket):
        self.client = client
        self.bucket = bucket

    def ensure_bucket(self):
        if not self.client.bucket_exists(self.bucket):
            raise ValueError(f"Create the destination bucket first: {self.bucket}")

    def exists(self, key):
        from minio.error import S3Error

        try:
            self.client.stat_object(self.bucket, key)
            return True
        except S3Error as error:
            if error.code in {"NoSuchKey", "NoSuchObject"}:
                return False
            raise

    def write(self, key, document):
        content = encode(document)
        self.client.put_object(self.bucket, key, io.BytesIO(content), len(content),
                               content_type="application/json")

    def read(self, key):
        response = self.client.get_object(self.bucket, key)
        try:
            return json.loads(response.read())
        finally:
            response.close()
            response.release_conn()

    def save(self, snapshot, box, collected_at):
        source = {"bounding_box": asdict(box), "response": snapshot}
        digest = hashlib.sha256(encode(source)).hexdigest()
        key = f"snapshots/opensky/{digest}.json"
        if not self.exists(key):
            self.write(key, {"schema_version": 1, "source_sha256": digest,
                             "collected_at": collected_at, **source})
        return key

    @staticmethod
    def receipt_key(key, topic):
        topic_id = hashlib.sha256(topic.encode()).hexdigest()
        snapshot_id = key.rsplit("/", 1)[-1]
        return f"published/{topic_id}/{snapshot_id}"

    def pending(self, topic):
        for item in self.client.list_objects(self.bucket, prefix="snapshots/opensky/", recursive=True):
            if item.object_name.endswith(".json") and not self.exists(self.receipt_key(item.object_name, topic)):
                yield item.object_name

    def published(self, key, topic, count):
        self.write(self.receipt_key(key, topic), {"snapshot_key": key, "topic": topic,
                                               "acknowledged": count})

    def load_verified(self, key):
        document = self.read(key)
        source = {"bounding_box": document["bounding_box"], "response": document["response"]}
        digest = hashlib.sha256(encode(source)).hexdigest()
        if document["source_sha256"] != digest or key != f"snapshots/opensky/{digest}.json":
            raise ValueError("Snapshot checksum mismatch")
        return document
