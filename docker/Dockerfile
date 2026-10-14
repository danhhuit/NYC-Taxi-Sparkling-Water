FROM python:3.11-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive

# ==========================================================
# JAVA 17
# ==========================================================
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps && \
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
    numpy==1.26.4 \
    PyYAML==6.0.2

# Final MOJO prediction script and shared configuration
COPY src/__init__.py src/__init__.py
COPY src/prediction/__init__.py src/prediction/__init__.py
COPY src/prediction/final_mojo_score.py src/prediction/final_mojo_score.py
COPY src/common/__init__.py src/common/__init__.py
COPY src/common/project.py src/common/project.py
COPY config/project_config.yaml config/project_config.yaml

# Batch input
COPY data/processed/batch_input.parquet \
    data/processed/test.parquet

# Exported MOJO model
COPY models/final_mojo \
    models/final_mojo

# Output directory
RUN mkdir -p outputs/predictions outputs/metrics

CMD ["python", "-m", "src.prediction.final_mojo_score", "--rows", "0", "--deployment-validation"]
