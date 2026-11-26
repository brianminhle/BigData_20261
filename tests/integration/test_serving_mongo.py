"""Mongo-backed failure/retry and visibility tests; use a disposable database."""

import copy
import uuid
from unittest.mock import patch
from pymongo.collection import Collection
from airtraffic.serving.mongo import connect_database, ServingStore

base = connect_database()
database = base.client.get_database('serving_test_' + uuid.uuid4().hex, write_concern=base.write_concern)
# Local tests may instead use an isolated collection set inside the app database.
store = ServingStore(database)
view = dict(generated_at=1000, latest_snapshot=900, hours=24, analytics_run='fixture-completed-run',
            windows=[dict(scope_id='a',window_start=900,observations=2,distinct_aircraft=1,with_position=2,stale_positions=0)],
            cells=[dict(scope_id='a',window_start=900,window_end=1200,cell_lat_min=20,cell_lon_min=105,distinct_aircraft=1)],
            movement=[dict(scope_id='a',icao24='abc123',observation_id='one',snapshot_time=900,time_position=890.,observation_gap=True)])
try:
    store.ensure_indexes()
    first = store.publish(view)
    assert store.overview({})['summary']['observations'] == 2
    assert store.publish(dict(view,generated_at=2000)) == first
    assert database.windows.count_documents({}) == 1
    updated = copy.deepcopy(view)
    updated['windows'][0]['observations'] = 3
    original = Collection.count_documents
    def fail_after_staging(collection, *args, **kwargs):
        if collection.name == 'cells':
            raise RuntimeError('Controlled interruption before commit')
        return original(collection, *args, **kwargs)
    with patch.object(Collection, 'count_documents', fail_after_staging):
        try:
            store.publish(updated)
            raise AssertionError('Expected injected failure')
        except RuntimeError:
            pass
    assert store.metadata()['generation'] == first
    assert store.overview({})['summary']['observations'] == 2
    restarted = ServingStore(database)
    second = restarted.publish(updated)
    assert second != first and restarted.overview({})['summary']['observations'] == 3
    assert database.windows.count_documents({'generation':second}) == 1
    assert len(restarted.movement({'icao24':['abc123']},gaps_only=True)['rows']) == 1
    assert len(restarted.movement({'icao24':['def456']})['rows']) == 0
    for params in ({'icao24':['$ne']},{'icao24':['abc123'],'limit':['9999']}):
        try:
            restarted.movement(params)
            raise AssertionError('Invalid input accepted')
        except ValueError:
            pass
    database.generations.update_one({'_id':first},{'$set':{'retired_at':1}})
    restarted.prune()
    assert database.windows.count_documents({'generation':first}) == 0
    assert restarted.overview({})['summary']['observations'] == 3
    print('MONGO_PUBLICATION_RECOVERY_PASS')
finally:
    base.client.drop_database(database.name)
    base.client.close()
