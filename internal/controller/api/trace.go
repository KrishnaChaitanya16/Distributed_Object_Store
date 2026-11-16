package api

import (
	"context"

	"github.com/KrishnaChaitanya16/Distributed_Object_Store/internal/controller/events"
)

type traceKey struct{}

// traceHeader lets a client label its request so the UI can follow it through the event log.
const traceHeader = "X-Trace-Id"

// withTrace tags ctx with a client-chosen trace id; an empty id leaves ctx unchanged.
func withTrace(ctx context.Context, id string) context.Context {
	if id == "" {
		return ctx
	}
	if len(id) > 64 {
		id = id[:64]
	}
	return context.WithValue(ctx, traceKey{}, id)
}

func traceOf(ctx context.Context) string {
	id, _ := ctx.Value(traceKey{}).(string)
	return id
}

// emit records a UI event, adding the request's trace id when it has one.
func emit(ctx context.Context, kind, msg string, kv ...any) {
	if id := traceOf(ctx); id != "" {
		kv = append(kv, "trace", id)
	}
	events.Emit(kind, msg, kv...)
}
