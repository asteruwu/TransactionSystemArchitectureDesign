# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# 1. 基础镜像保持 JDK 21
FROM eclipse-temurin:21.0.5_11-jdk AS builder

WORKDIR /app

# 🔥【关键】配置 Gradle 全局使用阿里云镜像源
# 我们直接写入一个 init.gradle 初始化脚本，Gradle 启动时会自动加载它
RUN mkdir -p ~/.gradle && \
    echo "allprojects { repositories { maven { url 'https://maven.aliyun.com/repository/public/' } } }" > ~/.gradle/init.gradle

COPY ["build.gradle", "gradlew", "./"]
COPY gradle gradle

# 赋予执行权限
RUN chmod +x gradlew

# 2. 下载依赖 (这时候就会飞快地走阿里云了)
RUN ./gradlew downloadRepos

COPY . .
RUN chmod +x gradlew
RUN ./gradlew installDist

# 3. 运行时镜像：建议统一用 JDK 21-alpine (原文件写的 25 可能是笔误，21 是 LTS 且匹配 builder)
FROM eclipse-temurin:21-jre-alpine

# 🔥【关键】Alpine 系统换源
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories
RUN apk update && apk add --no-cache ca-certificates

WORKDIR /app

# 复制构建产物 (这里保留原逻辑，把整个 app 目录拷过来，虽然略大但最稳)
COPY --from=builder /app .

EXPOSE 9555

# 启动命令
ENTRYPOINT ["/app/build/install/hipstershop/bin/AdService"]
