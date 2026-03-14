FROM golang:1.22-alpine AS build
WORKDIR /src
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -o /out/mcp-server .

FROM alpine:3.20
RUN adduser -D appuser
USER appuser
COPY --from=build /out/mcp-server /usr/local/bin/mcp-server
EXPOSE 8084
ENTRYPOINT ["/usr/local/bin/mcp-server"]
