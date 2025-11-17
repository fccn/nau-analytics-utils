FROM ubuntu:22.04
# Set Spark and Hadoop versions
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
# Install dependencies
RUN apt-get update && apt-get install -y \
   openjdk-11-jdk \
   curl \
   wget \
   bash \
   tini \
   ca-certificates \
   procps \
&& rm -rf /var/lib/apt/lists/*
# Set working directory
WORKDIR /opt
# Download and extract Spark
RUN wget https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
&& tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
&& mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark \
&& rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz
# Set Spark bin in PATH
ENV PATH="/opt/spark/bin:${PATH}"
# Set entrypoint
ENTRYPOINT ["/usr/bin/tini", "--", "/bin/bash"]