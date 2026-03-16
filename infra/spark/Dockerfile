FROM apache/spark:3.5.1-scala2.12-java11-python3-ubuntu

USER root

ENV SPARK_HOME=/opt/spark
ENV SPARK_VERSION=3.5.1

RUN curl -L https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar -o ${SPARK_HOME}/jars/mysql-connector-j-8.4.0.jar
RUN curl -L https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar -o ${SPARK_HOME}/jars/gcs-connector.jar

RUN chmod 644 ${SPARK_HOME}/jars/*.jar

USER spark
