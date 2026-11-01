FROM eclipse-temurin:21-jre
ARG GH_VERSION=11.0
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
WORKDIR /gh
RUN curl -fsSL -o graphhopper.jar \
    https://github.com/graphhopper/graphhopper/releases/download/${GH_VERSION}/graphhopper-web-${GH_VERSION}.jar
COPY config.yml entrypoint.sh ./
RUN chmod +x entrypoint.sh
EXPOSE 8989
ENTRYPOINT ["/gh/entrypoint.sh"]
