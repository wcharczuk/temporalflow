package temporalflow

import (
	"context"
	"fmt"

	"github.com/wcharczuk/go-incr"
	"go.temporal.io/sdk/workflow"
)

func GeneratorNode[Input, Output any](scope incr.Scope, activityType string, activityOptions *workflow.LocalActivityOptions) GeneratorNodeIncr[Output] {
	bind := &generator[Input, Output]{
		graph:           incr.ExpertScope(scope).ScopeGraph(),
		activityType:    activityType,
		activityOptions: activityOptions,
	}
	bindLeftChange := incr.WithinScope(scope, &bindLeftChangeIncr[Input, Output]{
		n:    incr.NewNode(incr.KindBindLHSChange),
		bind: bind,
		// we'll do this in the `SetInput` handler
		// parents: []incr.INode{input},
	})
	bind.lhsChange = bindLeftChange
	bindMain := incr.WithinScope(scope, &bindMainIncr[Input, Output]{
		n:       incr.NewNode(incr.KindBind),
		bind:    bind,
		parents: []incr.INode{bindLeftChange},
	})
	bind.main = bindMain
	incr.ExpertNode(bindLeftChange).SetOnErrorHandlers(append(incr.ExpertNode(bindLeftChange).OnErrorHandlers(), func(ctx context.Context, err error) {
		for _, eh := range incr.ExpertNode(bindMain).OnErrorHandlers() {
			eh(ctx, err)
		}
	}))
	incr.ExpertNode(bindLeftChange).SetOnAbortedHandlers(append(incr.ExpertNode(bindLeftChange).OnAbortedHandlers(), func(ctx context.Context, err error) {
		for _, eh := range incr.ExpertNode(bindMain).OnAbortedHandlers() {
			eh(ctx, err)
		}
	}))
	return bindMain
}

// GeneratorNodeIncr is a node that implements [incr.Bind], which can dynamically swap out
// subgraphs based on an input changing.
//
// GeneratorNodeIncr gives the graph dynamism, but as a result is somewhat expensive to
// compute and should be used tactically.
type GeneratorNodeIncr[Output any] interface {
	incr.BindIncr[Output]
	LocalActivityType() string
	LocalActivityOptions() *workflow.LocalActivityOptions
}

// generator is a root struct that holds shared
// information for both the main and the lhs-change.
type generator[A, B any] struct {
	graph    *incr.Graph
	lhs      incr.Incr[A]
	rhs      incr.Incr[B]
	rhsNodes []incr.INode

	// these should form the "bindFunc" implementation
	// but it is awkward in practice to do this, so we may
	// want to accept an activity node with the correct
	// type as the input at construction or setInput ...
	activityType    string
	activityOptions *workflow.LocalActivityOptions
	// fn        incr.BindContextFunc[A, B]

	main      *bindMainIncr[A, B]
	lhsChange *bindLeftChangeIncr[A, B]
}

func (b *generator[A, B]) LocalActivityType() string { return an.activityType }

func (b *generator[A, B]) LocalActivityOptions() *workflow.LocalActivityOptions {
	return b.activityOptions
}

func (b *generator[A, B]) AddInput(i incr.Incr[A]) error {
	b.lhsChange.parents = []incr.INode{i}
	return nil
	// an.inputs = append(an.inputs, i)
	// if incr.ExpertNode(an).Height() != incr.HeightUnset {
	// 	// if we're already part of the graph, we have
	// 	// to tell the graph to update our parent<>child metadata
	// 	return incr.ExpertGraph(incr.GraphForNode(an)).AddChild(an, i)
	// }
	// return nil
}

func (b *generator[A, B]) isTopScope() bool        { return false }
func (b *generator[A, B]) isScopeValid() bool      { return incr.ExpertNode(b.main).IsValid() }
func (b *generator[A, B]) isScopeNecessary() bool  { return incr.ExpertNode(b.main).IsNecessary() }
func (b *generator[A, B]) scopeGraph() *incr.Graph { return b.graph }
func (b *generator[A, B]) scopeHeight() int        { return incr.ExpertNode(b.lhsChange).Height() }
func (b *generator[A, B]) newIdentifier() incr.Identifier {
	return incr.ExpertGraph(b.graph).NewIdentifier()
}

func (b *generator[A, B]) addScopeNode(n incr.INode) {
	b.rhsNodes = append(b.rhsNodes, n)
}

func (b *generator[A, B]) String() string {
	return fmt.Sprintf("{%v}", b.main)
}

type bindMainIncr[A, B any] struct {
	n       *incr.Node
	bind    *generator[A, B]
	value   B
	parents []incr.INode
}

func (b *bindMainIncr[A, B]) Parents() (out []incr.INode) {
	return b.parents
}

func (b *bindMainIncr[A, B]) Stale() bool {
	return incr.ExpertNode(b).RecomputedAt() == 0 || incr.ExpertNode(b).IsStaleInRespectToParent()
}

func (b *bindMainIncr[A, B]) ShouldBeInvalidated() bool {
	return !incr.ExpertNode(b.bind.lhsChange).IsValid()
}

func (b *bindMainIncr[A, B]) Node() *incr.Node { return b.n }

func (b *bindMainIncr[A, B]) Value() (output B) {
	return b.value
}

func (b *bindMainIncr[A, B]) Stabilize(ctx context.Context) error {
	if b.bind.rhs != nil {
		b.value = b.bind.rhs.Value()
	} else {
		var zero B
		b.value = zero
	}
	return nil
}

func (b *bindMainIncr[A, B]) Invalidate() {
	for _, n := range b.bind.rhsNodes {
		incr.ExpertGraph(incr.GraphForNode(b)).InvalidateNode(n)
	}
}

func (b *bindMainIncr[A, B]) String() string {
	return b.n.String()
}

type bindLeftChangeIncr[A, B any] struct {
	n       *incr.Node
	bind    *generator[A, B]
	parents []incr.INode
}

func (b *bindLeftChangeIncr[A, B]) Parents() []incr.INode {
	return b.parents
}

func (b *bindLeftChangeIncr[A, B]) Node() *incr.Node { return b.n }

func (b *bindLeftChangeIncr[A, B]) ShouldBeInvalidated() bool {
	return !incr.ExpertNode(b.bind.lhs).IsValid()
}

func (b *bindLeftChangeIncr[A, B]) RightScopeNodes() []incr.INode {
	return b.bind.rhsNodes
}

func (b *bindLeftChangeIncr[A, B]) Stabilize(ctx context.Context) (err error) {
	oldRightNodes := b.bind.rhsNodes
	oldRhs := b.bind.rhs
	b.bind.rhsNodes = nil

	// we need to do the various things
	b.bind.rhs, err = b.bind.fn(ctx, b.bind, b.bind.lhs.Value())
	if err != nil {
		return
	}

	if b.bind.rhs != nil {
		b.bind.main.parents = []incr.INode{b, b.bind.rhs}
	} else {
		b.bind.main.parents = []incr.INode{b}
	}

	if err = incr.ExpertGraph(incr.GraphForNode(b)).ChangeParent(b.bind.main, oldRhs, b.bind.rhs); err != nil {
		return err
	}
	if oldRhs != nil {
		for _, n := range oldRightNodes {
			incr.ExpertGraph(incr.GraphForNode(b)).InvalidateNode(n)
		}
	}
	incr.ExpertGraph(incr.GraphForNode(b)).PropagateInvalidity()
	return nil
}

func (b *bindLeftChangeIncr[A, B]) String() string {
	return b.n.String()
}
