package redeo

import "context"

// Contextable interface for types own context
type Contextable interface {
	// Context retrieve context
	Context() context.Context

	// SetContext reset context
	SetContext(context.Context)
}
