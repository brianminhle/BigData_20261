"""Versioned serving data: stage rows first, atomically publish one metadata document."""

import hashlib
import json
import os
import re
import time

from pymongo import MongoClient, ReplaceOne
from pymongo.write_concern import WriteConcern
from airtraffic.serving.query import query_view, time_range

DATASETS = ('windows', 'cells', 'movement')


def connect_database():
    client = MongoClient(os.environ['AIRTRAFFIC_MONGO_URI'], serverSelectionTimeoutMS=5000,
                         connectTimeoutMS=5000, socketTimeoutMS=30000)
    return client.get_database(os.getenv('AIRTRAFFIC_MONGO_DATABASE', 'airtraffic'),
                               write_concern=WriteConcern(w='majority', j=True))


class ServingStore:
    def __init__(self, database):
        self.db = database

    def ensure_indexes(self):
        for name in ('windows', 'cells'):
            self.db[name].create_index([('generation', 1), ('scope_id', 1), ('window_start', 1)])
        self.db.movement.create_index([('generation', 1), ('scope_id', 1), ('icao24', 1),
                                      ('snapshot_time', 1), ('observation_id', 1)])

    def publish(self, view):
        # Content-addressed generations let interrupted retries reuse the same IDs.
        content = {key: value for key, value in view.items() if key != 'generated_at'}
        generation = hashlib.sha256(json.dumps(content, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()).hexdigest()
        active = self.db.publications.find_one({'_id': 'active'})
        if active and active['generation'] == generation:
            self.db.publications.update_one({'_id': 'active', 'generation': generation},
                                            {'$set': {'checked_at': int(time.time())}})
            return generation
        self.db.generations.update_one({'_id': generation}, {'$setOnInsert': {'created_at': int(time.time())}}, upsert=True)
        for name in DATASETS:
            rows = view.get(name, [])
            operations = []
            for index, row in enumerate(rows):
                document = dict(row, _id=f'{generation}:{index}', generation=generation)
                operations.append(ReplaceOne({'_id': document['_id']}, document, upsert=True))
                if len(operations) == 500:
                    self.db[name].bulk_write(operations, ordered=False)
                    operations = []
            if operations:
                self.db[name].bulk_write(operations, ordered=False)
            if self.db[name].count_documents({'generation': generation}) != len(rows):
                raise RuntimeError('Incomplete serving generation')
        metadata = {key: value for key, value in view.items() if key not in DATASETS}
        metadata.update(_id='active', generation=generation, published_at=int(time.time()),
                        checked_at=int(time.time()), scopes=sorted({row['scope_id'] for row in view['windows']}))
        if active:
            self.db.generations.update_one({'_id': active['generation']}, {'$set': {'retired_at': int(time.time())}}, upsert=True)
        self.db.generations.update_one({'_id': generation}, {'$unset': {'retired_at': ''}})
        self.db.publications.replace_one({'_id': 'active'}, metadata, upsert=True)
        return generation

    def prune(self, retain_seconds=3600):
        if retain_seconds < 300:
            raise ValueError('Retention must exceed API query timeouts')
        active = self.metadata()['generation']
        cutoff = int(time.time()) - retain_seconds
        candidates = {'_id': {'$ne': active}, '$or': [
            {'retired_at': {'$lt': cutoff}},
            {'retired_at': {'$exists': False}, 'created_at': {'$lt': cutoff}}]}
        for version in self.db.generations.find(candidates):
            for name in DATASETS:
                self.db[name].delete_many({'generation': version['_id']})
            self.db.generations.delete_one({'_id': version['_id']})

    def metadata(self):
        result = self.db.publications.find_one({'_id': 'active'}, {'_id': 0})
        if result is None:
            raise OSError('No completed serving publication')
        return result

    def overview(self, params):
        meta = self.metadata()
        scopes = meta['scopes']
        scope = params.get('scope', [scopes[0] if scopes else ''])[0]
        if scopes and scope not in scopes:
            raise ValueError('Unknown scope')
        start, end = time_range(meta['latest_snapshot'], params)
        predicate = {'generation': meta['generation'], 'scope_id': scope,
                     'window_start': {'$gte': start, '$lt': end}}
        for name in ('windows', 'cells'):
            rows = list(self.db[name].find(predicate, {'_id': 0, 'generation': 0})
                        .sort('window_start', 1).limit(20001).max_time_ms(5000))
            if len(rows) > 20000:
                raise ValueError('Query exceeds serving limit')
            meta[name] = rows
        result = query_view(meta, params)
        result.update({key: meta[key] for key in ('generation', 'published_at', 'checked_at')})
        result['analytics_run'] = meta.get('analytics_run')
        return result

    def movement(self, params, gaps_only=False):
        meta = self.metadata()
        icao = params.get('icao24', [''])[0].lower()
        if not re.fullmatch('[0-9a-f]{6}', icao):
            raise ValueError('icao24 must be six hexadecimal characters')
        scopes = meta['scopes']
        scope = params.get('scope', [scopes[0] if scopes else ''])[0]
        if scope not in scopes:
            raise ValueError('Unknown scope')
        start, end = time_range(meta['latest_snapshot'], params)
        limit, offset = int(params.get('limit', ['200'])[0]), int(params.get('offset', ['0'])[0])
        if not 1 <= limit <= 1000 or not 0 <= offset <= 100000:
            raise ValueError('Invalid pagination')
        predicate = {'generation': meta['generation'], 'scope_id': scope, 'icao24': icao,
                     'snapshot_time': {'$gte': start, '$lt': end}}
        if gaps_only:
            predicate['observation_gap'] = True
        rows = list(self.db.movement.find(predicate, {'_id': 0, 'generation': 0})
                    .sort([('snapshot_time', 1), ('observation_id', 1)]).skip(offset).limit(limit + 1).max_time_ms(5000))
        return dict(generation=meta['generation'], analytics_run=meta.get('analytics_run'),
                    rows=rows[:limit], has_more=len(rows) > limit, offset=offset, limit=limit)
