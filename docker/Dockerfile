FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive

# ==========================================================
# JAVA 17
# ==========================================================
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Java 17 compatibility for Sparkling Water
ENV JAVA_TOOL_OPTIONS="-Dstdout.encoding=UTF-8 -Dstderr.encoding=UTF-8 --add-exports=java.base/sun.net.www.protocol.http=ALL-UNNAMED --add-exports=java.base/sun.net.www.protocol.https=ALL-UNNAMED"

# ==========================================================
# PROJECT
# ==========================================================
WORKDIR /app

# Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    h2o-pysparkling-3.5==3.46.0.6.post1 \
    numpy==1.26.4

# MOJO prediction script
COPY src/20_load_mojo_predict.py src/20_load_mojo_predict.py

# Batch input
COPY data/processed/taxi_batch_input.parquet \
    data/processed/taxi_batch_input.parquet

# Exported MOJO model
COPY models/nyc_taxi_gbm_mojo \
    models/nyc_taxi_gbm_mojo

# Output directory
RUN mkdir -p outputs/predictions

CMD ["python", "src/20_load_mojo_predict.py"]