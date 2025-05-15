package temporalflow

import "context"

// IFinishStabilize is an interface that flow nodes can implement
// to e.g. await the completion of an activity or child workflow.
type IFinishStabilize interface {
	FinishStabilize(context.Context) error
}
