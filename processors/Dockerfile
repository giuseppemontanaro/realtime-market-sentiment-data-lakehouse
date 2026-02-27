# STAGE 1: Dependency Resolver
FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /app
COPY pom.xml .

RUN mvn dependency:copy-dependencies -DoutputDirectory=/app/spark-jars

# STAGE 2: Final Spark Image
FROM apache/spark:3.5.0
USER root

COPY --from=build /app/spark-jars/* /opt/spark/jars/

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY entrypoint.sh .
RUN chmod +x entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]