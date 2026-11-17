FROM golang:1.27-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /worker ./cmd/worker

FROM alpine:3.20
RUN apk --no-cache add ca-certificates iproute2
WORKDIR /app
COPY --from=builder /worker .
EXPOSE 8001
ENTRYPOINT ["/app/worker"]
