FROM golang:1.25-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/encache ./cmd/encache

FROM alpine:3.22

RUN addgroup -S encache && adduser -S -G encache encache

WORKDIR /app

COPY --from=build /out/encache /usr/local/bin/encache
RUN mkdir -p /app/storage && chown -R encache:encache /app/storage

USER encache

ENV HOST=0.0.0.0 \
    PORT=3000 \
    STORAGE_PATH=/app/storage

EXPOSE 3000
VOLUME ["/app/storage"]

ENTRYPOINT ["encache"]
