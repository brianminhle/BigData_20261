FROM spark:3.5.7-scala2.12-java17-python3-ubuntu@sha256:9ec659a98c6a11096c4ea18742af2f2e3963186ba3e958d132e59a78b6f584d0
USER root
RUN pip3 install --no-cache-dir kafka-python==2.2.15 \
    && mkdir -p /opt/spark/ivy && chown spark /opt/spark/ivy
USER spark
COPY deploy/spark/ivysettings.xml /opt/spark/ivysettings.xml
RUN printf 'print("Kafka connector resolved")\n' > /tmp/resolve.py \
    && /opt/spark/bin/spark-submit --conf spark.jars.ivy=/opt/spark/ivy \
       --conf spark.jars.ivySettings=/opt/spark/ivysettings.xml \
       --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7 /tmp/resolve.py \
    && rm /tmp/resolve.py
WORKDIR /workspace
ENV PYTHONPATH=/workspace HADOOP_USER_NAME=hadoop
