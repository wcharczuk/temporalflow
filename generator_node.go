package temporalflow

import (
	"context"

	"github.com/wcharczuk/go-incr"
)

type generatorNode[Input, Output any] struct {
	n     *incr.Node
	input incr.Incr[Input]
}

func (gn *generatorNode[Inputs, Output]) Stabilize(ctx context.Context) error {
	return nil
}
