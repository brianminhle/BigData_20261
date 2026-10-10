# This stage supplies files only; its x86 binaries are never executed on ARM.
FROM --platform=linux/amd64 apache/hadoop:3.4.2@sha256:0f79c18d27a2b15984868a41e245b09c98daa5638b573b4dbd848be650f05635 AS distribution
FROM eclipse-temurin:11-jre-jammy@sha256:1adc4db6079f9a35c45f36b0907dd94232a0b9b409a4088a7fe0c77b14e99294
COPY --from=distribution /opt/hadoop /opt/hadoop
RUN rm -rf /opt/hadoop/lib/native \
    && useradd --create-home --uid 1000 hadoop \
    && mkdir -p /var/lib/hadoop/name /var/lib/hadoop/data /opt/hadoop/logs \
    && chown -R hadoop:hadoop /var/lib/hadoop /opt/hadoop/logs

ENV HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV PATH="/opt/hadoop/bin:${PATH}" HADOOP_HEAPSIZE_MAX=384
COPY deploy/hdfs/core-site.xml deploy/hdfs/hdfs-site.xml /opt/hadoop/etc/hadoop/
COPY --chmod=755 deploy/hdfs/start.sh /usr/local/bin/start-hdfs
USER hadoop
ENTRYPOINT ["/usr/local/bin/start-hdfs"]
