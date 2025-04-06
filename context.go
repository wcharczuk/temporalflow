package temporalflow

import (
	"context"

	"go.temporal.io/sdk/workflow"
)

type workflowContextKey struct{}

func WithWorkflowContext(ctx context.Context, wfctx workflow.Context) context.Context {
	return context.WithValue(ctx, workflowContextKey{}, wfctx)
}

func GetWorkflowContext(ctx context.Context) workflow.Context {
	if value := ctx.Value(workflowContextKey{}); value != nil {
		if typed, ok := value.(workflow.Context); ok {
			return typed
		}
	}
	return nil
}
