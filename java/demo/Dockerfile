FROM eclipse-temurin:21-jdk

RUN apt-get update && apt-get install -y --no-install-recommends \
    erlang-base erlang-crypto erlang-public-key erlang-ssl erlang-tools \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY target/*.jar /app/app.jar

EXPOSE 8080 5005

# JDWP for IntelliJ remote debug + start epmd
ENV JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"

CMD ["bash", "-lc", "epmd -daemon && java -jar /app/app.jar"]
