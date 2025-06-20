# # syntax=docker/dockerfile:1
# FROM maven:3.8-openjdk-11 as build
# WORKDIR /build
# ADD . .
# RUN --mount=type=cache,target=/root/.m2 mvn package
# # RUN mvn package

# FROM openjdk:11-jre
# RUN apt update && \
#     apt install -y iputils-ping curl wget netcat python3 python3-pip git
# RUN pip3 install --upgrade --force-reinstall git+https://github.com/xtreme1-io/xtreme1-sdk.git@6b53a73
# WORKDIR /app
# COPY --from=build /build/target/xtreme1-backend-0.9.1-SNAPSHOT.jar ./app.jar
# RUN mkdir -p config
# RUN wget 'https://github.com/xtreme1-io/asset/raw/main/datasets/xtreme1-lidar-fusion-trial.zip' -O xtreme1-lidar-fusion-trial.zip
# RUN wget 'https://github.com/xtreme1-io/asset/raw/main/datasets/xtreme1-image-trial.zip' -O xtreme1-image-trial.zip
# EXPOSE 8080
# CMD java -jar app.jar


# 1. Build Stage
FROM maven:3.8-openjdk-11 as build
WORKDIR /build
COPY pom.xml ./
RUN --mount=type=cache,target=/root/.m2 mvn dependency:go-offline  # 의존성 미리 다운로드
COPY . .
RUN --mount=type=cache,target=/root/.m2 mvn package

# 2. Runtime Stage
FROM openjdk:11-jre
WORKDIR /app

# 3. 필수 패키지 미리 설치 (캐시 활용)
RUN apt update && apt install -y --no-install-recommends \
    iputils-ping curl wget netcat python3 python3-pip git && \
    rm -rf /var/lib/apt/lists/*  # 불필요한 캐시 제거

# 4. Python 패키지 설치 (최대한 캐시 활용)
RUN pip3 install --no-cache-dir --upgrade git+https://github.com/xtreme1-io/xtreme1-sdk.git@6b53a73

# 5. 애플리케이션 파일 복사
COPY --from=build /build/target/xtreme1-backend-0.9.1-SNAPSHOT.jar ./app.jar
RUN mkdir -p config

RUN mkdir -p config
# RUN wget 'https://github.com/xtreme1-io/asset/raw/main/datasets/xtreme1-lidar-fusion-trial.zip' -O xtreme1-lidar-fusion-trial.zip
# RUN wget 'https://github.com/xtreme1-io/asset/raw/main/datasets/xtreme1-image-trial.zip' -O xtreme1-image-trial.zip
EXPOSE 8080
CMD java -jar app.jar