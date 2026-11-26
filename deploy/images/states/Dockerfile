# Reuse the pinned base already used by the repository; this job does not start Spark.
FROM spark:3.5.7-scala2.12-java17-python3-ubuntu@sha256:9ec659a98c6a11096c4ea18742af2f2e3963186ba3e958d132e59a78b6f584d0
USER root
WORKDIR /app
COPY pyproject.toml setup.cfg /app/
COPY src /app/src
RUN pip3 install --no-cache-dir .
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 185
ENTRYPOINT ["airtraffic-collect-states"]
