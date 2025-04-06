package temporalflow

import (
	"context"
	"fmt"

	"github.com/wcharczuk/go-incr"
)

// Observe returns a observe increment.
func Observe[A any](graph *incr.Graph) incr.ObserveIncr[A] {
	o := incr.WithinScope(graph, &observeIncr[A]{
		n: incr.NewNode("observer"),
	})
	return o
}

type observeIncr[A any] struct {
	n        *incr.Node
	observed incr.Incr[A]
}

func (o *observeIncr[A]) OnUpdate(fn func(context.Context, A)) {
	o.n.OnUpdate(func(ctx context.Context) {
		fn(ctx, o.Value())
	})
}

func (o *observeIncr[A]) AddInput(observed incr.Incr[A]) error {
	o.observed = observed
	return incr.ExpertGraph(incr.GraphForNode(o)).ObserveNode(o, observed)
}

func (o *observeIncr[A]) Node() *incr.Node { return o.n }

func (o *observeIncr[A]) Unobserve(ctx context.Context) {
	incr.ExpertGraph(incr.GraphForNode(o)).UnobserveNode(o, o.observed)
	o.observed = nil
}

func (o *observeIncr[A]) Value() (output A) {
	if o.observed == nil {
		return
	}
	return o.observed.Value()
}

func (o *observeIncr[A]) String() string {
	if o.n.Label() != "" {
		return fmt.Sprintf("%s[%s]:%s", o.n.Kind(), o.n.ID().Short(), o.n.Label())
	}
	return fmt.Sprintf("%s[%s]", o.n.Kind(), o.n.ID().Short())
}
