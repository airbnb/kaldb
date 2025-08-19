FROM maven:3-amazoncorretto-21 AS build

RUN yum install -y tar gzip wget
RUN wget https://github.com/async-profiler/async-profiler/releases/download/v4.1/async-profiler-4.1-linux-arm64.tar.gz
RUN tar -zxvf *.tar.gz

COPY . /work/
RUN cd /work; mvn package -DskipTests

FROM amazoncorretto:21
COPY --from=build /work/astra/target/astra.jar /
COPY --from=build /work/config/config.yaml /
COPY --from=build /work/config/schema.yaml /
ENTRYPOINT [ "java", "-Xms512m", "-Xmx2g", "--enable-preview"]
CMD [ "-jar", "./astra.jar", "config.yaml" ]
