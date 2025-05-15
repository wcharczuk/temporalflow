package temporalflow

import (
	"context"

	"github.com/wcharczuk/go-incr"
)

type generatorNode[Inputs, Output any] struct {
	n      *incr.Node
	inputs []incr.Incr[Inputs]
}

func (gn *generatorNode[Inputs, Output]) Stabilize(ctx context.Context) error {
	return nil
}

func (gn *generatorNode[Inputs, Output]) FinishStabilize(ctx context.Context) error {
	return nil
}
