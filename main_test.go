package temporalflow

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/wcharczuk/go-incr"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func Test_E2E(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	defer env.AssertExpectations(t)
	wf := Orchestrator{}
	env.RegisterWorkflow(wf.Orchestrate)
	env.RegisterActivity(greeter)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalSetVariable, SignalSetVariableArgs{
			Selector: NodeSelector{
				Label: "name",
			},
			Value: "not-bufo",
		})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalStabilize, SignalStabilizeArgs{})
	}, time.Second)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(SignalQuit, SignalQuitArgs{})
	}, 2*time.Second)

	graph := makeGraph()
	env.ExecuteWorkflow(wf.Orchestrate, graph)
	err := env.GetWorkflowError()
	if err != nil {
		t.Errorf("execution failed %v", err)
		t.FailNow()
	}

	val, err := env.QueryWorkflow(QueryValues)
	if err != nil {
		t.Errorf("query workflow failed %v", err)
		t.FailNow()
	}
	var output = make(QueryValuesReturn)
	err = val.Get(&output)
	if err != nil {
		t.Errorf("deserializing query result failed %v", err)
		t.FailNow()
	}
	obsValue, ok := output["obs"]
	if !ok {
		t.Error("output observer node not found in query output")
		t.Errorf("graph: %#v", output)
		t.FailNow()
	}

	expected := "Hello not-bufo!"
	if typedObsValue, _ := obsValue.(string); typedObsValue != expected {
		t.Errorf(`expected observer value to be %q, was %q`, expected, obsValue)
		t.FailNow()
	}
}

func greeter(ctx context.Context, name string) (string, error) {
	return fmt.Sprintf("Hello %s!", name), nil
}

func makeGraph() (g SerializedGraph) {
	g.ID = incr.NewIdentifier()
	nameVar := SerializedNode{
		Kind:  NodeKindVariable,
		Label: "name",
		Value: "Bufo",
	}
	obs := SerializedNode{
		Kind:  NodeKindObserver,
		Label: "obs",
	}
	var greetNodes []SerializedNode
	for index := range 3 {
		greetNodes = append(greetNodes, SerializedNode{
			Kind:         NodeKindActivity,
			Label:        fmt.Sprintf("greet_%02d", index),
			ActivityType: "greeter",
			ActivityOptions: &workflow.ActivityOptions{
				StartToCloseTimeout: 10 * time.Second,
				RetryPolicy: &temporal.RetryPolicy{
					InitialInterval:    5 * time.Second,
					BackoffCoefficient: 1.0,
					MaximumAttempts:    5,
				},
			},
		})
	}
	g.Nodes = append([]SerializedNode{
		nameVar,
		obs,
	}, greetNodes...)
	for index := range 3 {
		g.Edges = append(g.Edges, SerializedEdge{
			From: NodeSelector{Label: nameVar.Label},
			To:   NodeSelector{Label: fmt.Sprintf("greet_%02d", index)},
		})
		g.Edges = append(g.Edges, SerializedEdge{
			From: NodeSelector{Label: fmt.Sprintf("greet_%02d", index)},
			To:   NodeSelector{Label: obs.Label},
		})
	}
	return
}
