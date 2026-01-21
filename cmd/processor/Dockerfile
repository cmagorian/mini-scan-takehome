FROM golang:1.20-bullseye

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=linux go build -o processor ./cmd/processor/main.go
RUN mkdir -p /app/data

CMD ["./processor"]
