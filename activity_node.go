package temporalflow

import (
	"context"
	"fmt"

	"github.com/wcharczuk/go-incr"
	"go.temporal.io/sdk/workflow"
)

// ActivityNode returns a new activity node.
func ActivityNode[Inputs, Output any](scope incr.Scope, activityType string, activityOptions workflow.ActivityOptions) ActivityNodeIncr[Inputs, Output] {
	return incr.WithinScope(scope, &activityNode[Inputs, Output]{
		n:               incr.NewNode("activity"),
		activityType:    activityType,
		activityOptions: activityOptions,
	})
}

var (
	_ incr.Incr[string]          = (*activityNode[int, string])(nil)
	_ incr.MapNIncr[int, string] = (*activityNode[int, string])(nil)
	_ incr.INode                 = (*activityNode[int, string])(nil)
	_ incr.IStabilize            = (*activityNode[int, string])(nil)
	_ fmt.Stringer               = (*activityNode[int, string])(nil)
)

type ActivityNodeIncr[Inputs, Output any] interface {
	incr.MapNIncr[Inputs, Output]

	ActivityType() string
	ActivityOptions() workflow.ActivityOptions
}

type activityNode[Inputs, Output any] struct {
	n               *incr.Node
	activityType    string
	activityOptions workflow.ActivityOptions

	inputs []incr.Incr[Inputs]
	val    Output
}

func (an *activityNode[Inputs, Output]) ActivityType() string { return an.activityType }
func (an *activityNode[Inputs, Output]) ActivityOptions() workflow.ActivityOptions {
	return an.activityOptions
}

func (an *activityNode[Inputs, Output]) Parents() []incr.INode {
	output := make([]incr.INode, len(an.inputs))
	for i := 0; i < len(an.inputs); i++ {
		output[i] = an.inputs[i]
	}
	return output
}

func (an *activityNode[Inputs, Output]) AddInput(i incr.Incr[Inputs]) error {
	an.inputs = append(an.inputs, i)
	if incr.ExpertNode(an).Height() != incr.HeightUnset {
		// if we're already part of the graph, we have
		// to tell the graph to update our parent<>child metadata
		return incr.ExpertGraph(incr.GraphForNode(an)).AddChild(an, i)
	}
	return nil
}

func (an *activityNode[Inputs, Output]) RemoveInput(id incr.Identifier) error {
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

func (an *activityNode[Inputs, Output]) Node() *incr.Node { return an.n }

func (an *activityNode[Inputs, Output]) Value() Output { return an.val }

func (an *activityNode[Inputs, Output]) Stabilize(ctx context.Context) (err error) {
	var val Output
	values := make([]any, len(an.inputs))
	for index := range an.inputs {
		values[index] = an.inputs[index].Value()
	}
	wctx := GetWorkflowContext(ctx)
	wctx = workflow.WithActivityOptions(wctx, an.activityOptions)
	fut := workflow.ExecuteActivity(wctx, an.activityType, values...)
	err = fut.Get(wctx, &val)
	if err != nil {
		return
	}
	an.val = val
	return
}

func (an *activityNode[Inputs, Output]) String() string {
	return an.n.String()
}

func remove[A incr.INode](nodes []A, id incr.Identifier) (output []A, removed A) {
	output = make([]A, 0, len(nodes))
	for _, n := range nodes {
		if n.Node().ID() != id {
			output = append(output, n)
		} else {
			removed = n
		}
	}
	return
}
