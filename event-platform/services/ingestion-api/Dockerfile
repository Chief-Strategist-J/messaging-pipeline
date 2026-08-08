FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /ingestion-api ./cmd/server

FROM golang:1.23
WORKDIR /app
COPY --from=build /ingestion-api /ingestion-api
COPY . .
EXPOSE 8080
ENTRYPOINT ["/ingestion-api"]
