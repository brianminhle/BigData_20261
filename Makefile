.PHONY: image config test test-unit test-silver test-streaming

image:
	docker compose build producer

config:
	docker compose config --quiet

test: image
	$(MAKE) test-unit test-silver

test-unit:
	docker compose run --no-deps --rm -v "$(CURDIR)/tests:/workspace/tests:ro" --entrypoint /opt/spark/bin/spark-submit batch --master 'local[1]' tests/unit/test_configuration.py
	docker compose run --no-deps --rm -v "$(CURDIR)/tests:/workspace/tests:ro" --entrypoint python3 batch -m unittest discover -s tests/unit -p 'test_raw_*.py' -v
	docker compose run --no-deps --rm -v "$(CURDIR)/tests:/workspace/tests:ro" --entrypoint python3 batch -m unittest discover -s tests/unit -p 'test_opensky_*.py' -v

test-silver:
	docker compose run --no-deps --rm -v "$(CURDIR)/tests:/workspace/tests:ro" --entrypoint /opt/spark/bin/spark-submit batch --master 'local[1]' --driver-memory 512m tests/integration/test_silver.py

# Requires the Kafka/HDFS stack; writes isolated fixture runs to those services.
test-streaming:
	docker compose run --no-deps --rm -v "$(CURDIR)/tests:/workspace/tests:ro" --entrypoint /opt/spark/bin/spark-submit stream --master 'local[2]' --driver-memory 1536m --conf spark.jars.ivy=/opt/spark/ivy --conf spark.jars.ivySettings=/opt/spark/ivysettings.xml --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 tests/integration/test_streaming.py

# Current state-vector pipeline; independent from the legacy root Compose stack.
STATES_COMPOSE = docker compose -f deploy/local/compose.yaml
.PHONY: states-config states-build states-up states-check-api states-recover states-report states-test states-stop
states-config:
	$(STATES_COMPOSE) config --quiet
states-build:
	$(STATES_COMPOSE) build init
states-up: states-config states-build
	$(STATES_COMPOSE) up -d minio kafka
	$(STATES_COMPOSE) run --rm init
states-check-api:
	$(STATES_COMPOSE) run --rm --no-deps collector --once --anonymous --insecure
states-recover:
	$(STATES_COMPOSE) run --rm --no-deps collector --replay-pending --insecure
states-report:
	mkdir -p data/state-reports
	$(STATES_COMPOSE) run --rm --no-deps batch --insecure --output /output/run-$$(date +%Y%m%dT%H%M%S)
states-test:
	$(STATES_COMPOSE) run --rm --no-deps tests
states-stop:
	$(STATES_COMPOSE) stop

.PHONY: states-stream-build states-stream-once states-stream-test
states-stream-build:
	docker compose -f deploy/local/compose.yaml build stream

# Run stages in order; their checkpoints resume on subsequent invocations.
states-stream-once:
	@for stage in archive dedup windows; do \
		docker compose -f deploy/local/compose.yaml run --rm --no-deps stream \
		--stage $$stage --output s3a://airtraffic-states/stream-v1 --insecure --available-now || exit $$?; \
	done

states-stream-test:
	docker compose -f deploy/local/compose.yaml run --rm --no-deps \
		-v "$(CURDIR)/tests:/tests:ro" --entrypoint /opt/spark/bin/spark-submit \
		stream --master 'local[2]' /tests/integration/test_state_stream.py

.PHONY: states-check-auth
states-check-auth:
	docker compose -f deploy/local/compose.yaml run --rm --no-deps \
		-v "$(CURDIR)/credentials.json:/run/secrets/opensky.json:ro" \
		collector --once --insecure --credentials-file /run/secrets/opensky.json

.PHONY: dashboard-export dashboard-serve
dashboard-export:
	mkdir -p data/dashboard
	chmod g+w data/dashboard
	docker compose -f deploy/local/compose.yaml run --rm --no-deps \
		--user "185:$$(id -g)" \
		-v "$(CURDIR)/src:/app/src:ro" -e PYTHONPATH=/app/src \
		-v "$(CURDIR)/data/dashboard:/output" --entrypoint /opt/spark/bin/spark-submit \
		stream --master 'local[2]' --conf spark.jars.ivy=/tmp/ivy /app/src/airtraffic/jobs/export_dashboard.py \
		--output /output/view.json --insecure

dashboard-serve:
	PYTHONPATH=src python3 -m airtraffic.serving.server

.PHONY: analytics-test analytics-run analytics-config
analytics-test:
	docker compose -f deploy/local/compose.yaml run --rm --no-deps \
		-v "$(CURDIR)/src:/app/src:ro" -v "$(CURDIR)/tests:/tests:ro" -e PYTHONPATH=/app/src \
		--entrypoint /opt/spark/bin/spark-submit stream --master 'local[2]' /tests/integration/test_analytics.py

analytics-run:
	@test -n "$(START)" -a -n "$(END)" -a -n "$(OUTPUT)" || (echo 'Set START, END, OUTPUT'; exit 1)
	docker compose -f deploy/local/compose.yaml run --rm --no-deps \
		--entrypoint /opt/spark/bin/spark-submit stream --master 'local[2]' \
		/app/src/airtraffic/jobs/analyze_states.py --start "$(START)" --end "$(END)" --output "$(OUTPUT)" --insecure

analytics-config:
	kubectl kustomize deploy/k8s/apps/analytics

.PHONY: serving-build serving-up serving-stop serving-test runtime-config
serving-build:
	docker compose -f deploy/local/compose.yaml build api publisher

serving-up:
	docker compose -f deploy/local/compose.yaml up -d minio mongo api publisher analytics-worker

serving-stop:
	docker compose -f deploy/local/compose.yaml stop analytics-worker publisher api mongo minio

serving-test:
	docker compose -f deploy/local/compose.yaml run --rm --no-deps \
		-v "$(CURDIR)/tests:/tests:ro" --entrypoint /opt/spark/bin/spark-submit \
		publisher --master 'local[1]' /tests/integration/test_serving_lake.py

runtime-config:
	kubectl kustomize deploy/k8s/apps/runtime

.PHONY: runtime-build runtime-local-up runtime-local-stop
runtime-build:
	docker compose -f deploy/local/compose.yaml build collector publisher api

# Explicit opt-in: this starts real API collection, unlike serving-up.
runtime-local-up:
	docker compose -f deploy/local/compose.yaml -f deploy/local/runtime.yaml up -d \
		minio kafka init collector stream mongo publisher api analytics-worker

runtime-local-stop:
	docker compose -f deploy/local/compose.yaml -f deploy/local/runtime.yaml stop
