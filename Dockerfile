FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    ACM_LAKE=/data/lake DAGSTER_HOME=/opt/dagster
WORKDIR /app

# config/ and contracts/ are read from the source tree (acm.pricing.ROOT), so the package is
# installed in place.
COPY pyproject.toml ./
COPY src ./src
COPY config ./config
COPY contracts ./contracts
RUN pip install --no-cache-dir -e ".[dev]" && mkdir -p /opt/dagster /data/lake

ENTRYPOINT ["acm"]
CMD ["--help"]
