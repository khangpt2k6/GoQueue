FROM golang:1.24-alpine AS builder
WORKDIR /app

COPY go.mod ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/broker ./cmd/broker

FROM alpine:3.20
RUN adduser -D -g '' goqueue
WORKDIR /home/goqueue
COPY --from=builder /bin/broker /usr/local/bin/broker
USER goqueue
ENTRYPOINT ["broker"]
