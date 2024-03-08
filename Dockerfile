FROM amd64/maven:3-amazoncorretto-21 as build
COPY . /work/
RUN cd /work; mvn package -DskipTests

FROM amd64/maven:3-amazoncorretto-21
COPY --from=build /work/kaldb/target/kaldb.jar /
COPY --from=build /work/config/config.yaml /
ENTRYPOINT [ "java", "--enable-preview", "-jar", "./kaldb.jar", "config.yaml" ]
