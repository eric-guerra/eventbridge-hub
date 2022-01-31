FROM maven:3.8-openjdk-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q dependency:go-offline
COPY src src
RUN mvn -q package -DskipTests

FROM eclipse-temurin:17-jre
COPY --from=build /src/target/eventbridge-hub-1.0.0.jar /app.jar
ENTRYPOINT ["java", "-jar", "/app.jar"]
