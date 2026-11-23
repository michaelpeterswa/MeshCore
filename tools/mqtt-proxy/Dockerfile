# syntax=docker/dockerfile:1
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /mqtt-proxy ./cmd/mqtt-proxy

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /mqtt-proxy /mqtt-proxy
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/mqtt-proxy", "-config", "/etc/mqtt-proxy/config.yaml"]
