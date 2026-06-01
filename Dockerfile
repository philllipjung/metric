# Stage 1: Build
FROM golang:1.26-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY exporter_cr_v1.go .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o exporter_cr_v1 exporter_cr_v1.go

# Stage 2: Runtime
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy the binary from builder
COPY --from=builder /app/exporter_cr_v1 .

# Expose metrics port
EXPOSE 9302

# Run the exporter
CMD ["./exporter_cr_v1"]
