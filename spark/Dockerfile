FROM openjdk:8-slim

ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN apt-get update && apt-get install -y curl bash && rm -rf /var/lib/apt/lists/* \
    && curl -L "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
       | tar -xz -C /opt \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME}

ENV PATH="${SPARK_HOME}/bin:${PATH}"
WORKDIR /opt/spark/work-dir

CMD ["tail", "-f", "/dev/null"]
