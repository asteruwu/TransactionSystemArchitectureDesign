FROM golang:1.24.0-alpine AS builder

WORKDIR /app

COPY go.mod .
COPY go.sum .

RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -o cartservice .

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/cartservice .

EXPOSE 7070

CMD ["./cartservice"]