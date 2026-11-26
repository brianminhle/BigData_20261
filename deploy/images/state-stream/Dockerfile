FROM spark:3.5.7-scala2.12-java17-python3-ubuntu@sha256:9ec659a98c6a11096c4ea18742af2f2e3963186ba3e958d132e59a78b6f584d0
USER root
COPY deploy/images/spark/ivysettings.xml /opt/spark/ivysettings.xml
RUN printf 'print("Connectors resolved")\n' > /tmp/resolve.py \
    && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/opt/spark/ivy \
       --conf spark.jars.ivySettings=/opt/spark/ivysettings.xml \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7,org.apache.hadoop:hadoop-aws:3.3.4 /tmp/resolve.py \
    && cp /opt/spark/ivy/jars/*.jar /opt/spark/jars/ && rm /tmp/resolve.py
WORKDIR /app
COPY pyproject.toml setup.cfg /app/
COPY src /app/src
RUN pip3 install --no-cache-dir .
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 185
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--master", "local[2]", "/app/src/airtraffic/jobs/stream_states.py"]
