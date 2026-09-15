# ---------- Build stage ----------
    FROM golang:1.26 AS builder

    WORKDIR /app
    
    # Copy dependency files first so Docker can cache downloads.
    COPY go.mod go.sum ./
    RUN go mod download
    
    # Copy the application source.
    COPY . .
    
    # Build the PayFlow API.
    RUN CGO_ENABLED=0 GOOS=linux go build -o payflow-api ./cmd/api
    
    
    # ---------- Runtime stage ----------
    FROM alpine:3.22
    
    WORKDIR /app
    
    # Add CA certificates for HTTPS calls.
    RUN apk add --no-cache ca-certificates
    
    # Copy only the compiled binary from the build stage.
    COPY --from=builder /app/payflow-api .
    
    # PayFlow API port.
    EXPOSE 8080
    
    # Start the API.
    CMD ["./payflow-api"]