FROM openjdk:11-jre-slim
COPY ./build/libs/biocache-service-3.2.1-SNAPSHOT.war app.war
CMD ["java", "-jar", "-Dserver.port=8080", "app.war"]