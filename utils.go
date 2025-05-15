package temporalflow

import "github.com/wcharczuk/go-incr"

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
