FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /out/ingestion ./main.go

FROM alpine:3.20
RUN adduser -D appuser
USER appuser
COPY --from=build /out/ingestion /usr/local/bin/ingestion
EXPOSE 8082
ENTRYPOINT ["/usr/local/bin/ingestion"]
