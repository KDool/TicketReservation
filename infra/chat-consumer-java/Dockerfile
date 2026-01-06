FROM maven:3.9.7-eclipse-temurin-21 AS build

WORKDIR /src
COPY pom.xml .
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre

WORKDIR /app
COPY --from=build /src/target/chat-consumer-0.1.0.jar /app/chat-consumer.jar

CMD ["java", "-jar", "/app/chat-consumer.jar"]
