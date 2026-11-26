FROM python:3.11-slim-bookworm@sha256:a83ba18aef0f865b1a4a611c2cf460fe446f2795673bbb9fe43be570439e3b39
WORKDIR /app
COPY pyproject.toml setup.cfg /app/
COPY src /app/src
RUN pip install --no-cache-dir . && useradd --uid 10001 --no-create-home app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 10001
EXPOSE 18080
ENTRYPOINT ["python", "-m", "airtraffic.serving.server", "--host", "0.0.0.0"]
