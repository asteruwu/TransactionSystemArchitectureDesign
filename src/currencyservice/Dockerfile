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

# 1. 去掉 @sha256，只保留版本号，避免元数据下载失败
FROM node:20.18.1-alpine AS builder

# 2. 【新增】Alpine 系统换源 (解决 apk add python3/make/g++ 超时)
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories

# Some packages (e.g. @google-cloud/profiler) require additional
# deps for post-install scripts
RUN apk add --update --no-cache \
    python3 \
    make \
    g++

WORKDIR /usr/src/app

COPY package*.json ./

# 3. 【保留】NPM 换源 (你之前写的对的，继续保留)
RUN npm install --only=production \
    --registry=https://registry.npmmirror.com \
    --grpc_binary_host_mirror=https://registry.npmmirror.com/-/binary/grpc/ \
    --verbose

# -----------------------------------------------

# 4. 第二阶段：运行时镜像，也去掉 @sha256
FROM alpine:3.20

# 5. 【新增】运行时环境换源 (解决 apk add nodejs 超时)
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories

RUN apk add --no-cache nodejs

WORKDIR /usr/src/app

COPY --from=builder /usr/src/app/node_modules ./node_modules

COPY . .

EXPOSE 7000

ENTRYPOINT [ "node", "server.js" ]