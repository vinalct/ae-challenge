FROM python:3.11-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    JAVA_HOME=/usr/lib/jvm/default-java \
    PYTHONPATH=/workspace/src \
    SPARK_LOCAL_IP=127.0.0.1

ENV PATH="$JAVA_HOME/bin:$PATH"

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        bash \
        default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

WORKDIR /workspace
CMD ["bash"]
