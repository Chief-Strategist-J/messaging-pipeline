FROM golang:1.23-alpine AS build
RUN apk add --no-cache git
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /ingestion-api ./cmd/server

FROM build AS test
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go test ./src/features/events/tests/... -v

FROM alpine:3.20
RUN apk add --no-cache graphviz ca-certificates tzdata
WORKDIR /app
COPY --from=build /ingestion-api /ingestion-api
EXPOSE 8080
ENTRYPOINT ["/ingestion-api"]
