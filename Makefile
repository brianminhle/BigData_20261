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
