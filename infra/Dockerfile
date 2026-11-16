FROM python:3.11.15-slim-bookworm@sha256:d29f48a31a8b408ed19272ca1e7b10ebae13b240a27e862d3d4217c528e2e0c3

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless ca-certificates \
    && ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /opt/java \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/opt/java \
    PYSPARK_PYTHON=/usr/local/bin/python \
    PYTHONPATH=/workspace/src \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    SPARK_LOCAL_IP=127.0.0.1

WORKDIR /workspace
COPY requirements.txt /opt/requirements.txt
RUN pip install --no-cache-dir --require-hashes -r /opt/requirements.txt
CMD ["python", "scripts/check_environment.py"]
