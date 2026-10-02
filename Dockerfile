FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/airlock ./cmd/airlock

FROM alpine:3.21
RUN adduser -D -u 10001 airlock
COPY --from=build /out/airlock /usr/local/bin/airlock
USER airlock
ENTRYPOINT ["airlock"]
