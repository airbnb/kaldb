# ---- build ----
FROM maven:3-amazoncorretto-21 as build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -e -DskipTests dependency:resolve
COPY src ./src
RUN mvn -q -DskipTests package

# ---- run ----
FROM maven:3-amazoncorretto-21 as build
WORKDIR /app
# The shaded JAR is named tools-loadgen-…; copying the wrong prefix leaves loadgen.jar missing.
COPY --from=build /src/target/tools-loadgen-*-jar-with-dependencies.jar /app/loadgen.jar
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseZGC"
ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar /app/loadgen.jar"]
