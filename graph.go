package temporalflow

import (
	"fmt"

	"github.com/wcharczuk/go-incr"
	"go.temporal.io/sdk/workflow"
)

type SerializedGraph struct {
	ID               incr.Identifier
	Label            string
	StabilizationNum uint64
	Nodes            []Node
	Edges            []Edge
	RecomputeHeap    []incr.Identifier
}

func (sg SerializedGraph) FlowGraph() (g FlowGraph, err error) {
	g.Graph = incr.New(
		incr.OptGraphDeterministic(true),
	)
	if !sg.ID.IsZero() {
		incr.ExpertGraph(g.Graph).SetID(sg.ID)
	}
	g.Graph.SetLabel(sg.Label)
	incr.ExpertGraph(g.Graph).SetStabilizationNum(sg.StabilizationNum)

	activityLookup := make(map[incr.Identifier]incr.INode)
	g.NodeLookup = make(map[incr.Identifier]incr.INode, len(sg.Nodes))
	g.NodeLabelLookup = make(map[string]incr.Identifier, len(sg.Nodes))
	for _, n := range sg.Nodes {
		var parsed incr.INode
		switch n.Kind {
		case string(NodeKindVariable):
			parsedVar := incr.Var(g.Graph, n.Var.Value)
			g.Variables = append(g.Variables, parsedVar)
			parsed = parsedVar
		case string(NodeKindActivity):
			parsedActivity := ActivityNode[any, any](g.Graph, n.Activity.ActivityType, n.Activity.ActivityOptions)
			activityLookup[parsedActivity.Node().ID()] = parsedActivity
			parsed = parsedActivity
		case string(NodeKindObserver):
			parsedObserver := Observe[any](g.Graph)
			g.Observers = append(g.Observers, parsedObserver)
			parsed = parsedObserver
		default:
			continue
		}
		if !n.ID.IsZero() {
			incr.ExpertNode(parsed).SetID(n.ID)
		}
		parsed.Node().SetLabel(n.Label)
		incr.ExpertNode(parsed).SetSetAt(n.SetAt)
		incr.ExpertNode(parsed).SetChangedAt(n.ChangedAt)
		incr.ExpertNode(parsed).SetRecomputedAt(n.RecomputedAt)
		g.NodeLookup[parsed.Node().ID()] = parsed
		if n.Label != "" {
			g.NodeLabelLookup[n.Label] = parsed.Node().ID()
		}
	}
	for _, e := range sg.Edges {
		fromID := e.FromID
		if fromID.IsZero() {
			fromID = g.NodeLabelLookup[e.FromLabel]
		}
		if fromID.IsZero() {
			continue
		}
		toID := e.ToID
		if toID.IsZero() {
			toID = g.NodeLabelLookup[e.ToLabel]
		}
		if toID.IsZero() {
			continue
		}
		fromNode, ok := g.NodeLookup[fromID]
		if !ok {
			err = fmt.Errorf("from node with id %s not found", e.FromID)
			return
		}
		toNode, ok := g.NodeLookup[toID]
		if !ok {
			err = fmt.Errorf("to node with id %s not found", e.ToID)
			return
		}
		typedForAddNode, ok := toNode.(IAddInput[any])
		if !ok {
			err = fmt.Errorf("to node with id %s cannot add nodes", e.ToID)
			return
		}
		typedForAddNode.AddInput(fromNode.(incr.Incr[any]))
	}
	for _, nodeID := range sg.RecomputeHeap {
		n, ok := g.NodeLookup[nodeID]
		if ok {
			incr.ExpertGraph(g.Graph).RecomputeHeapAdd(n)
		}
	}
	return
}

type IAddInput[A any] interface {
	AddInput(incr.Incr[A]) error
}

type FlowGraph struct {
	Graph           *incr.Graph
	NodeLookup      map[incr.Identifier]incr.INode
	NodeLabelLookup map[string]incr.Identifier
	Variables       []incr.VarIncr[any]
	Observers       []incr.ObserveIncr[any]
}

func (fg FlowGraph) Serialize() (output SerializedGraph) {
	output.ID = fg.Graph.ID()
	output.StabilizationNum = incr.ExpertGraph(fg.Graph).StabilizationNum()
	output.Label = fg.Graph.Label()
	output.RecomputeHeap = incr.ExpertGraph(fg.Graph).RecomputeHeapIDs()
	for _, n := range fg.NodeLookup {
		output.Nodes = append(output.Nodes, serializeNode(n))
		for _, p := range incr.ExpertNode(n).Parents() {
			output.Edges = append(output.Edges, Edge{
				FromID:    p.Node().ID(),
				FromLabel: p.Node().Label(),
				ToID:      n.Node().ID(),
				ToLabel:   n.Node().Label(),
			})
		}
		for _, o := range incr.ExpertNode(n).Observers() {
			output.Edges = append(output.Edges, Edge{
				FromID:    o.Node().ID(),
				FromLabel: o.Node().Label(),
				ToID:      n.Node().ID(),
				ToLabel:   n.Node().Label(),
			})
		}
	}
	return
}

func serializeNode(n incr.INode) (output Node) {
	output.ID = n.Node().ID()
	output.Kind = n.Node().Kind()
	output.Label = n.Node().Label()
	output.SetAt = incr.ExpertNode(n).SetAt()
	output.RecomputedAt = incr.ExpertNode(n).RecomputedAt()
	output.ChangedAt = incr.ExpertNode(n).ChangedAt()
	output.NumChanges = incr.ExpertNode(n).NumChanges()
	output.NumRecomputes = incr.ExpertNode(n).NumRecomputes()

	switch n.Node().Kind() {
	case string(NodeKindVariable):
		typed, ok := n.(incr.VarIncr[any])
		if !ok {
			return
		}
		output.Var.Value = typed.Value()
	case string(NodeKindActivity):
		typed, ok := n.(ActivityNodeIncr[any, any])
		if !ok {
			return
		}
		output.Activity.ActivityType = typed.ActivityType()
		output.Activity.ActivityOptions = typed.ActivityOptions()
	case string(NodeKindObserver):
		// do nothing
	}
	return
}

type Node struct {
	ID    incr.Identifier
	Label string
	Kind  string

	SetAt        uint64
	ChangedAt    uint64
	RecomputedAt uint64

	NumRecomputes uint64
	NumChanges    uint64

	Activity Activity
	Var      Var
}

type NodeKind string

var (
	NodeKindVariable NodeKind = incr.KindVar
	NodeKindActivity NodeKind = "activity"
	NodeKindObserver NodeKind = incr.KindObserver
)

type Edge struct {
	FromID    incr.Identifier
	FromLabel string
	ToID      incr.Identifier
	ToLabel   string
}

type Var struct {
	Value any
}

type Activity struct {
	ActivityType    string
	ActivityOptions workflow.ActivityOptions
}
