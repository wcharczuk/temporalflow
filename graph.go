package temporalflow

import (
	"fmt"

	"github.com/wcharczuk/go-incr"
	"go.temporal.io/sdk/workflow"
)

// SerializedGraph is the serialized form of a graph.
type SerializedGraph struct {
	ID                     incr.Identifier
	Label                  string
	Status                 int32
	StabilizationNum       uint64
	Nodes                  []SerializedNode
	Edges                  []SerializedEdge
	RecomputeHeap          []incr.Identifier
	SetDuringStabilization []incr.Identifier
}

// SerializedNode is a node in a graph.
type SerializedNode struct {
	ID    incr.Identifier
	Label string
	Kind  NodeKind

	Height                    *int
	HeightInRecomputeHeap     *int
	HeightInAdjustHeightsHeap *int

	SetAt        uint64
	ChangedAt    uint64
	RecomputedAt uint64

	NumRecomputes uint64
	NumChanges    uint64

	Value any

	ActivityType    string
	ActivityOptions *workflow.ActivityOptions

	LocalActivityType    string
	LocalActivityOptions *workflow.LocalActivityOptions
}

// NodeKind is a kind of node, or a meta class of node
// and determines how the graph should interact with the node
// during stabilization.
type NodeKind string

var (
	// NodeKindVariable is a variable or input node.
	NodeKindVariable NodeKind = incr.KindVar
	// NodeKindActivity is a node that takes inputs and produces an output.
	NodeKindActivity NodeKind = "activity"
	// NodeKindLocalActivity is a node that takes inputs and produces an output but runs locally.
	NodeKindLocalActivity NodeKind = "local_activity"
	// NodeKindObserver is a node that takes inputs and marks the nodes that
	// produced them as "observed" or relevant for the computation.
	NodeKindObserver NodeKind = incr.KindObserver
)

// SerializedEdge is a serialized form of the state that tracks if two nodes are connected.
type SerializedEdge struct {
	From NodeSelector
	To   NodeSelector
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
		case NodeKindVariable:
			parsedVar := incr.Var(g.Graph, n.Value)
			g.Variables = append(g.Variables, parsedVar)
			parsed = parsedVar
		case NodeKindActivity:
			parsedActivity := ActivityNode[any, any](g.Graph, n.ActivityType, n.ActivityOptions)
			activityLookup[parsedActivity.Node().ID()] = parsedActivity
			parsed = parsedActivity
		case NodeKindLocalActivity:
			parsedActivity := LocalActivityNode[any, any](g.Graph, n.LocalActivityType, n.LocalActivityOptions)
			activityLookup[parsedActivity.Node().ID()] = parsedActivity
			parsed = parsedActivity
		case NodeKindObserver:
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

		// only do this ... ???
		if n.Height != nil {
			incr.ExpertNode(parsed).SetHeight(*n.Height)
		}
		if n.HeightInRecomputeHeap != nil {
			incr.ExpertNode(parsed).SetHeightInRecomputeHeap(*n.HeightInRecomputeHeap)
		}
		if n.HeightInAdjustHeightsHeap != nil {
			incr.ExpertNode(parsed).SetHeightInAdjustHeightsHeap(*n.HeightInAdjustHeightsHeap)
		}
		if setValue, ok := parsed.(ISetValue[any]); ok {
			setValue.SetValue(n.Value)
		}

		g.NodeLookup[parsed.Node().ID()] = parsed
		if n.Label != "" {
			g.NodeLabelLookup[n.Label] = parsed.Node().ID()
		}
	}
	for _, e := range sg.Edges {
		fromID := e.From.ID
		if fromID.IsZero() {
			fromID = g.NodeLabelLookup[e.From.Label]
		}
		if fromID.IsZero() {
			continue
		}
		toID := e.To.ID
		if toID.IsZero() {
			toID = g.NodeLabelLookup[e.To.Label]
		}
		if toID.IsZero() {
			continue
		}
		fromNode, ok := g.NodeLookup[fromID]
		if !ok {
			err = fmt.Errorf("from node with id %s not found", e.From.ID)
			return
		}
		toNode, ok := g.NodeLookup[toID]
		if !ok {
			err = fmt.Errorf("to node with id %s not found", e.To.ID)
			return
		}
		typedForAddNode, ok := toNode.(IAddInput[any])
		if !ok {
			err = fmt.Errorf("to node with id %s cannot add nodes", e.To.ID)
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

type ISetValue[A any] interface {
	SetValue(A)
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
			output.Edges = append(output.Edges, SerializedEdge{
				From: NodeSelector{
					ID:    p.Node().ID(),
					Label: p.Node().Label(),
				},
				To: NodeSelector{
					ID:    n.Node().ID(),
					Label: n.Node().Label(),
				},
			})
		}
		for _, o := range incr.ExpertNode(n).Observers() {
			output.Edges = append(output.Edges, SerializedEdge{
				From: NodeSelector{
					ID:    n.Node().ID(),
					Label: n.Node().Label(),
				},
				To: NodeSelector{
					ID:    o.Node().ID(),
					Label: o.Node().Label(),
				},
			})
		}
	}
	return
}

func serializeNode(n incr.INode) (output SerializedNode) {
	output.ID = n.Node().ID()
	output.Kind = NodeKind(n.Node().Kind())
	output.Label = n.Node().Label()

	output.Height = ptrTo(incr.ExpertNode(n).Height())
	output.HeightInRecomputeHeap = ptrTo(incr.ExpertNode(n).HeightInRecomputeHeap())
	output.HeightInAdjustHeightsHeap = ptrTo(incr.ExpertNode(n).HeightInAdjustHeightsHeap())
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
		output.Value = typed.Value()
	case string(NodeKindActivity):
		typed, ok := n.(ActivityNodeIncr[any, any])
		if !ok {
			return
		}
		output.ActivityType = typed.ActivityType()
		output.ActivityOptions = typed.ActivityOptions()
		output.Value = typed.Value()
	case string(NodeKindLocalActivity):
		typed, ok := n.(LocalActivityNodeIncr[any, any])
		if !ok {
			return
		}
		output.LocalActivityType = typed.LocalActivityType()
		output.LocalActivityOptions = typed.LocalActivityOptions()
		output.Value = typed.Value()
	case string(NodeKindObserver):
		// do nothing
	}
	return
}

func ptrTo[A any](v A) *A {
	return &v
}
