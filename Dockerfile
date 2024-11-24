FROM openjdk:17-jdk-slim
WORKDIR /app
ARG JAR_FILE=./build/libs/*-SNAPSHOT.jar
COPY ${JAR_FILE} app.jar
COPY .env .env
EXPOSE 8080

CMD ["java", "-jar", "app.jar"]
