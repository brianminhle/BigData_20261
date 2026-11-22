FROM spark:3.5.7-scala2.12-java17-python3-ubuntu@sha256:9ec659a98c6a11096c4ea18742af2f2e3963186ba3e958d132e59a78b6f584d0
USER root
COPY pyproject.toml /workspace/pyproject.toml
COPY setup.cfg /workspace/setup.cfg
COPY src /workspace/src
WORKDIR /workspace
RUN test -f pyproject.toml && test -f setup.cfg && test -f src/airtraffic/__init__.py
RUN pip3 install --no-cache-dir . \
    && python3 -c "import airtraffic, fastavro, kafka, pyModeS" \
    && mkdir -p /opt/spark/ivy && chown spark /opt/spark/ivy
USER spark
COPY deploy/images/spark/ivysettings.xml /opt/spark/ivysettings.xml
RUN printf 'print("Kafka connector resolved")\n' > /tmp/resolve.py \
    && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/opt/spark/ivy \
       --conf spark.jars.ivySettings=/opt/spark/ivysettings.xml \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /tmp/resolve.py \
    && rm /tmp/resolve.py
ENV HADOOP_USER_NAME=hadoop
