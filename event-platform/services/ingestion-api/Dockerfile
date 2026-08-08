FROM golang:1.23 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /ingestion-api ./cmd/server

FROM gcr.io/distroless/static-debian12
COPY --from=build /ingestion-api /ingestion-api
EXPOSE 8080
ENTRYPOINT ["/ingestion-api"]
