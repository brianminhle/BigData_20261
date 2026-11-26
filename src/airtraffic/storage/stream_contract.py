"""Prevent accidental checkpoint reuse with incompatible stream settings."""

from dataclasses import asdict
import json


def verify_contract(spark, args):
    contract = json.dumps({'version': 1, 'topic': args.topic,
                           'bootstrap_servers': args.bootstrap_servers,
                           'bbox': asdict(args.box), 'watermark': args.watermark,
                           'window_seconds': 300, 'cell_degrees': 1}, sort_keys=True)
    path = spark._jvm.org.apache.hadoop.fs.Path(args.output.rstrip('/') + '/_contract')
    filesystem = path.getFileSystem(spark._jsc.hadoopConfiguration())
    if filesystem.exists(path):
        stream = filesystem.open(path)
        try:
            if stream.readUTF() != contract:
                raise ValueError('Stream configuration changed: use a new output root and checkpoints')
        finally:
            stream.close()
        return
    filesystem.mkdirs(path.getParent())
    stream = filesystem.create(path, False)
    try:
        stream.writeUTF(contract)
    finally:
        stream.close()
