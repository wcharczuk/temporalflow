package temporalflow

import (
	"context"

	"go.temporal.io/sdk/workflow"
)

type workflowContextKey struct{}

// WithWorkflowContext adds a [workflow.Context] to a normal context.
func WithWorkflowContext(ctx context.Context, wfctx workflow.Context) context.Context {
	return context.WithValue(ctx, workflowContextKey{}, wfctx)
}

// GetWorkflowContext returns the [workflow.Context] that would have been added
// by [WithWorkflowContext] or <nil> if it's not present.
func GetWorkflowContext(ctx context.Context) workflow.Context {
	if value := ctx.Value(workflowContextKey{}); value != nil {
		if typed, ok := value.(workflow.Context); ok {
			return typed
		}
	}
	return nil
}
