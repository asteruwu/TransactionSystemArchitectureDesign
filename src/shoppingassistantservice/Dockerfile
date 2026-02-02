# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# 1. 去掉 @sha256 锁死，防止元数据校验超时
FROM python:3.12.8-slim AS base

FROM base AS builder

# 2. 【关键】Debian 系统换源 (把官方源换成阿里云)
# 注意：Python 3.12 slim 基于 Debian Bookworm，源文件位置变了，必须改这个文件
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources

# 3. 安装系统依赖
RUN apt-get -qq update \
    && apt-get install -y --no-install-recommends g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# 4. 【关键】Pip 换源 (使用清华源 HTTP 直连，避开 SSL 错误)
RUN pip install --upgrade pip setuptools wheel -i http://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn
RUN pip install -r requirements.txt -i http://pypi.tuna.tsinghua.edu.cn/simple --trusted-host pypi.tuna.tsinghua.edu.cn --no-cache-dir

FROM base
# Enable unbuffered logging
ENV PYTHONUNBUFFERED=1

# get packages
WORKDIR /shoppingassistantservice

# Grab packages from builder
COPY --from=builder /usr/local/lib/python3.12/ /usr/local/lib/python3.12/

# Add the application
COPY . .

# set listen port
ENV PORT "8080"
EXPOSE 8080

ENTRYPOINT ["python", "shoppingassistantservice.py"]
