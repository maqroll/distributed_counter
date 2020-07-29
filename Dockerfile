FROM maven:3.6.1-jdk-8-alpine AS MAVEN_BUILD

COPY ./ ./

RUN mvn clean install

FROM openjdk:8-jre-alpine3.9

COPY --from=MAVEN_BUILD target/distributed.counter-0.1-SNAPSHOT.jar /distributed-counter.jar
COPY --from=MAVEN_BUILD target/lib/jgroups-4.2.1.Final.jar  /jgroups.jar
COPY --from=MAVEN_BUILD target/lib/slf4j-api-1.7.30.jar  /slf4j-api.jar
COPY --from=MAVEN_BUILD target/lib/slf4j-simple-1.7.30.jar  /slf4j-simple.jar

WORKDIR /

CMD ["java", "-cp", "/distributed-counter.jar:/jgroups.jar:/slf4j-api.jar:/slf4j-simple.jar", "test.JGroupNode"]