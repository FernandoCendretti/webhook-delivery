package observability

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

type ctxKey int

const requestIDKey ctxKey = iota

// LoggerConfig controls the log level and output format.
type LoggerConfig struct {
	Level  string
	Format string
}

// NewLogger creates a structured logger that writes to stderr.
func NewLogger(cfg LoggerConfig) *slog.Logger {
	return NewLoggerWithWriter(cfg, os.Stderr)
}

// NewLoggerWithWriter creates a structured logger that writes to w.
// Format "text" selects the text handler; any other value selects JSON.
func NewLoggerWithWriter(cfg LoggerConfig, w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{Level: parseLevel(cfg.Level)}

	var handler slog.Handler
	switch strings.ToLower(cfg.Format) {
	case "text":
		handler = slog.NewTextHandler(w, opts)
	default:
		handler = slog.NewJSONHandler(w, opts)
	}
	return slog.New(handler)
}

// WithRequestID stores the request ID in ctx so it can be retrieved by
// RequestIDFrom or injected into log lines by FromContext.
func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, requestIDKey, id)
}

// RequestIDFrom returns the request ID stored in ctx, or an empty string if
// none was set.
func RequestIDFrom(ctx context.Context) string {
	if v, ok := ctx.Value(requestIDKey).(string); ok {
		return v
	}
	return ""
}

// FromContext returns base augmented with the request_id attribute when one is
// present in ctx.
func FromContext(ctx context.Context, base *slog.Logger) *slog.Logger {
	if id := RequestIDFrom(ctx); id != "" {
		return base.With("request_id", id)
	}
	return base
}

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}
