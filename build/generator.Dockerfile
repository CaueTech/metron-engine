# Stage 1: Build binary using official Go compiler
FROM golang:alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Compile static Linux binary without CGO dependencies
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/generator ./cmd/generator

# Stage 2: Minimal runtime image
FROM alpine:latest

# Install certificates for external TLS connections
RUN apk --no-cache add ca-certificates

WORKDIR /root/

# Copy only the compiled binary from the builder stage
COPY --from=builder /bin/generator .

ENTRYPOINT ["./generator"]