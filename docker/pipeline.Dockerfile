# Spark 3.5 runs on Java 17 or 21; pinning it here is why the Docker path works
# on any host regardless of which JDK happens to be installed.
FROM eclipse-temurin:17-jre-jammy

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    RP_PROJECT_ROOT=/opt/pipeline \
    PYTHONPATH=/opt/pipeline/src

RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 python3-pip python3-venv procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/local/bin/python

WORKDIR /opt/pipeline

COPY requirements.txt requirements-dev.txt ./
RUN pip install --break-system-packages -r requirements-dev.txt

COPY pyproject.toml ./
COPY conf ./conf
COPY src ./src
COPY tests ./tests
COPY dags ./dags
RUN pip install --break-system-packages --no-deps -e .

ENTRYPOINT ["rewards"]
CMD ["--help"]
