package temporalflow

import (
	"context"
	"fmt"

	"github.com/wcharczuk/go-incr"
	"go.temporal.io/sdk/workflow"
)

// LocalActivityNode returns a new local activity node.
func LocalActivityNode[Inputs, Output any](scope incr.Scope, localActivityType string, localActivityOptions *workflow.LocalActivityOptions) LocalActivityNodeIncr[Inputs, Output] {
	return incr.WithinScope(scope, &localActivityNode[Inputs, Output]{
		n:               incr.NewNode(string(NodeKindActivity)),
		activityType:    localActivityType,
		activityOptions: localActivityOptions,
	})
}

// LocalActivityNodeIncr is the interface that local activtiy nodes implement.
type LocalActivityNodeIncr[Inputs, Output any] interface {
	incr.MapNIncr[Inputs, Output]
	LocalActivityType() string
	LocalActivityOptions() *workflow.LocalActivityOptions
}

var (
	_ incr.Incr[string]          = (*activityNode[int, string])(nil)
	_ incr.MapNIncr[int, string] = (*activityNode[int, string])(nil)
	_ incr.INode                 = (*activityNode[int, string])(nil)
	_ incr.IStabilize            = (*activityNode[int, string])(nil)
	_ fmt.Stringer               = (*activityNode[int, string])(nil)
)

type localActivityNode[Inputs, Output any] struct {
	n               *incr.Node
	activityType    string
	activityOptions *workflow.LocalActivityOptions

	inputs []incr.Incr[Inputs]
	val    Output
}

func (an *localActivityNode[Inputs, Output]) LocalActivityType() string { return an.activityType }

func (an *localActivityNode[Inputs, Output]) LocalActivityOptions() *workflow.LocalActivityOptions {
	return an.activityOptions
}

func (an *localActivityNode[Inputs, Output]) Parents() []incr.INode {
	output := make([]incr.INode, len(an.inputs))
	for i := range an.inputs {
		output[i] = an.inputs[i]
	}
	return output
}

func (an *localActivityNode[Inputs, Output]) AddInput(i incr.Incr[Inputs]) error {
	an.inputs = append(an.inputs, i)
	if incr.ExpertNode(an).Height() != incr.HeightUnset {
		// if we're already part of the graph, we have
		// to tell the graph to update our parent<>child metadata
		return incr.ExpertGraph(incr.GraphForNode(an)).AddChild(an, i)
	}
	return nil
}

func (an *localActivityNode[Inputs, Output]) RemoveInput(id incr.Identifier) error {
	var removed incr.Incr[Inputs]
	an.inputs, removed = remove(an.inputs, id)
	if removed != nil {
		incr.ExpertNode(an).RemoveParent(id)
		incr.ExpertNode(removed).RemoveChild(an.n.ID())
		incr.GraphForNode(an).SetStale(an)
		incr.ExpertGraph(incr.GraphForNode(an)).CheckIfUnnecessary(removed)
		return nil
	}
	return nil
}

func (an *localActivityNode[Inputs, Output]) Node() *incr.Node { return an.n }

func (an *localActivityNode[Inputs, Output]) SetValue(value Output) {
	an.val = value
}

func (an *localActivityNode[Inputs, Output]) Value() Output { return an.val }

func (an *localActivityNode[Inputs, Output]) Stabilize(ctx context.Context) (err error) {
	values := make([]any, len(an.inputs))
	for index := range an.inputs {
		values[index] = an.inputs[index].Value()
	}
	wctx := GetWorkflowContext(ctx)
	if an.activityOptions != nil {
		wctx = workflow.WithLocalActivityOptions(wctx, *an.activityOptions)
	}
	err = workflow.ExecuteLocalActivity(wctx, an.activityType, values...).Get(wctx, &an.val)
	return
}

func (an *localActivityNode[Inputs, Output]) String() string {
	return an.n.String()
}
