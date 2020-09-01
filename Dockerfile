FROM maven:3.6.1-jdk-8-alpine AS MAVEN_BUILD
WORKDIR /build
COPY pom.xml .
RUN mvn dependency:go-offline

COPY src/ /build/src/

RUN mvn clean install

FROM openjdk:8-jre-alpine3.9

COPY --from=MAVEN_BUILD /build/target/distributed.counter-0.1-SNAPSHOT.jar /distributed-counter.jar
COPY --from=MAVEN_BUILD /build/target/lib/aeron-all-1.29.0.jar  /aeron-all.jar
COPY --from=MAVEN_BUILD /build/target/lib/aeron-agent-1.29.0.jar  /aeron-agent.jar
#COPY --from=MAVEN_BUILD /build/target/lib/slf4j-api-1.7.30.jar  /slf4j-api.jar
#COPY --from=MAVEN_BUILD /build/target/lib/slf4j-simple-1.7.30.jar  /slf4j-simple.jar

WORKDIR /

CMD ["java", "-cp", "/distributed-counter.jar:/aeron-all.jar", \
        "-javaagent:/aeron-agent.jar", \
        "-Daeron.event.cluster.log=all", \
        "test.AeronNode"]