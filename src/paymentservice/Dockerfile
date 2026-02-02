# 第一阶段：构建
FROM node:20.18.1-alpine AS builder

# 1. 换源 (Builder 阶段)
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories

RUN apk add --update --no-cache \
    python3 \
    make \
    g++

WORKDIR /usr/src/app

COPY package*.json ./

# 2. NPM 换源
RUN npm install --only=production --registry=https://registry.npmmirror.com

# -----------------------------------------------

# 第二阶段：运行
# 3. 修改这里：去掉了 @sha256:xxx 这种死板的写法，直接用版本号
FROM alpine:3.20

# 4. 换源 (运行阶段也需要，否则 apk add nodejs 会卡死)
RUN sed -i 's/dl-cdn.alpinelinux.org/mirrors.aliyun.com/g' /etc/apk/repositories

# 5. 安装 Node.js 运行时
RUN apk add --no-cache nodejs

WORKDIR /usr/src/app

COPY --from=builder /usr/src/app/node_modules ./node_modules
COPY . .

EXPOSE 50051

ENTRYPOINT [ "node", "index.js" ]
