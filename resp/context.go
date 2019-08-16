package resp

import (
	"context"
)

// Context is the set of metadata that is passed for every command.
type CommandContext struct {
	ClientID       uint64
}

// Prevents collisions with keys defined in other packages.
type key struct{}

// Instance of key
var contextKey = &key{}

// NewContext returns a new Context that carries value lc.
func NewContext(parent context.Context, ctx *CommandContext) context.Context {
	return context.WithValue(parent, contextKey, ctx)
}

// FromContext returns the LambdaContext value stored in ctx, if any.
func FromContext(ctx context.Context) (*CommandContext, bool) {
	c, ok := ctx.Value(contextKey).(*CommandContext)
	return c, ok
}
