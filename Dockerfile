# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Stage 1 — builder
# ---------------------------------------------------------------------------
FROM golang:1.25-bookworm AS builder

WORKDIR /src

# Cache dependency downloads separately from source compilation.
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build a statically-linked binary.
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/webhookd ./cmd/webhookd

# ---------------------------------------------------------------------------
# Stage 2 — runtime
# Distroless "nonroot" image: no shell, no package manager, minimal attack
# surface.  The binary is the only executable in the image.
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

# Copy the binary from the builder stage.
COPY --from=builder /out/webhookd /webhookd

# The binary supports three subcommands: api, worker, scheduler.
# Override ENTRYPOINT args at runtime, e.g.:
#   docker run webhookd worker
#   docker run webhookd scheduler
ENTRYPOINT ["/webhookd"]
CMD ["api"]

# ---- Exposed ports (documentation only; actual binding is via env vars) ---
# API:     $API_PORT     (default 8080)
# Metrics: $METRICS_PORT (default 9090)
EXPOSE 8080 9090
