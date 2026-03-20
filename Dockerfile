# syntax=docker/dockerfile:1

FROM golang:1.22-alpine AS builder
WORKDIR /app

RUN apk add --no-cache gcc musl-dev

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o /anchordb ./cmd/anchordb

FROM alpine:3.20
WORKDIR /app

RUN apk add --no-cache ca-certificates tzdata postgresql16-client mysql-client sqlite

COPY --from=builder /anchordb /usr/local/bin/anchordb

EXPOSE 8080
ENTRYPOINT ["anchordb"]
