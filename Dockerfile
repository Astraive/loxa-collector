# Build stage
FROM golang:1.20-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go env -w GOPROXY=https://proxy.golang.org
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags "-s -w" -o /loxa-collector ./cmd/loxa-collector

# Final stage
FROM gcr.io/distroless/static
COPY --from=build /loxa-collector /loxa-collector
ENTRYPOINT ["/loxa-collector"]
