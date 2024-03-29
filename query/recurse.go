// Portions Copyright 2017-2018 Dgraph Labs, Inc. are available under the Apache License v2.0.
// Portions Copyright 2022 Outcaste LLC are available under the Sustainable License v1.0.

package query

import (
	"context"
	"math"
	"strconv"

	"github.com/outcaste-io/outserv/algo"
	"github.com/outcaste-io/outserv/codec"
	"github.com/outcaste-io/outserv/x"
	"github.com/outcaste-io/sroar"
	"github.com/pkg/errors"
)

func (start *SubGraph) expandRecurse(ctx context.Context, maxDepth uint64) error {
	// Note: Key format is - "attr|fromUID|toUID"
	reachMap := make(map[string]*sroar.Bitmap)
	allowLoop := start.Params.RecurseArgs.AllowLoop
	var numEdges uint64
	var exec []*SubGraph
	var err error

	rrch := make(chan error, len(exec))
	startChildren := make([]*SubGraph, len(start.Children))
	copy(startChildren, start.Children)
	// Empty children before giving to ProcessGraph as we are only concerned with DestUids.
	start.Children = start.Children[:0]

	// Process the root first.
	go ProcessGraph(ctx, start, nil, rrch)
	select {
	case err = <-rrch:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}

	if start.UnknownAttr {
		return nil
	}

	// Add children back and expand if necessary
	if exec, err = expandChildren(ctx, start, startChildren); err != nil {
		return err
	}

	dummy := &SubGraph{}
	var depth uint64
	for {
		if depth >= maxDepth {
			return nil
		}
		depth++

		rrch := make(chan error, len(exec))
		for _, sg := range exec {
			go ProcessGraph(ctx, sg, dummy, rrch)
		}

		var recurseErr error
		for range exec {
			select {
			case err = <-rrch:
				if err != nil {
					if recurseErr == nil {
						recurseErr = err
					}
				}
			case <-ctx.Done():
				if recurseErr == nil {
					recurseErr = ctx.Err()
				}
			}
		}

		if recurseErr != nil {
			return recurseErr
		}

		for _, sg := range exec {
			// sg.uidMatrix can be empty. Continue if that is the case.
			if len(sg.uidMatrix) == 0 {
				continue
			}

			if sg.UnknownAttr {
				continue
			}

			if len(sg.Filters) > 0 {
				// We need to do this in case we had some filters.
				sg.updateUidMatrix()
			}

			for mIdx, fromUID := range codec.GetUids(sg.SrcUIDs) {
				if allowLoop {
					// TODO: This needs to be optimized.
					for _, ul := range sg.uidMatrix {
						numEdges += codec.ListCardinality(ul)
					}
				} else {
					ul := sg.uidMatrix[mIdx]
					ur := codec.FromListNoCopy(ul)
					if ur.IsEmpty() {
						continue
					}

					key := sg.Attr + "|" + strconv.Itoa(int(fromUID))
					prev, ok := reachMap[key]
					if !ok {
						reachMap[key] = codec.FromList(ul)
						continue
					}
					// Any edges that we have seen before, do not consider
					// them again.
					if len(sg.uidMatrix[mIdx].SortedUids) > 0 {
						// we will have to keep the order, so using ApplyFilter
						algo.ApplyFilter(sg.uidMatrix[mIdx], func(uid uint64, i int) bool {
							if ur.Contains(uid) {
								return false
							}
							numEdges++
							return true
						})
					} else {
						ur.AndNot(prev) // This would only keep the UIDs which are NEW.
						sg.uidMatrix[mIdx].Bitmap = ur.ToBuffer()
						numEdges += uint64(ur.GetCardinality())

						prev.Or(ur) // Add the new UIDs to our "reach"
						reachMap[key] = prev
					}
				}
			}
			if len(sg.Params.Order) > 0 {
				// Can't use merge sort if the UIDs are not sorted.
				sg.updateDestUids()
			} else {
				sg.DestMap = codec.Merge(sg.uidMatrix)
			}
		}

		// modify the exec and attach child nodes.
		var out []*SubGraph
		var exp []*SubGraph
		for _, sg := range exec {
			if sg.UnknownAttr {
				continue
			}
			if sg.DestMap.IsEmpty() {
				continue
			}
			if exp, err = expandChildren(ctx, sg, startChildren); err != nil {
				return err
			}
			out = append(out, exp...)
		}

		if numEdges > x.Config.LimitQueryEdge {
			// If we've seen too many edges, stop the query.
			return errors.Errorf("Exceeded query edge limit = %v. Found %v edges.",
				x.Config.LimitQueryEdge, numEdges)
		}

		if len(out) == 0 {
			return nil
		}
		exec = out
	}
}

// expandChildren adds child nodes to a SubGraph with no children, expanding them if necessary.
func expandChildren(ctx context.Context, sg *SubGraph, children []*SubGraph) ([]*SubGraph, error) {
	if len(sg.Children) > 0 {
		return nil, errors.New("Subgraph should not have any children")
	}
	// Add children and expand if necessary
	sg.Children = append(sg.Children, children...)
	expandedChildren, err := expandSubgraph(ctx, sg)
	if err != nil {
		return nil, err
	}
	out := make([]*SubGraph, 0, len(expandedChildren))
	sg.Children = sg.Children[:0]
	// Link new child nodes back to parent destination UIDs
	for _, child := range expandedChildren {
		newChild := new(SubGraph)
		newChild.copyFiltersRecurse(child)
		newChild.SrcUIDs = codec.ToList(sg.DestMap)
		newChild.Params.Var = child.Params.Var
		sg.Children = append(sg.Children, newChild)
		out = append(out, newChild)
	}
	return out, nil
}

func recurse(ctx context.Context, sg *SubGraph) error {
	if !sg.Params.Recurse {
		return errors.Errorf("Invalid recurse path query")
	}

	depth := sg.Params.RecurseArgs.Depth
	if depth == 0 {
		if sg.Params.RecurseArgs.AllowLoop {
			return errors.Errorf("Depth must be > 0 when loop is true for recurse query")
		}
		// If no depth is specified, expand till we reach all leaf nodes
		// or we see reach too many nodes.
		depth = math.MaxUint64
	}

	for _, child := range sg.Children {
		if len(child.Children) > 0 {
			return errors.Errorf(
				"recurse queries require that all predicates are specified in one level")
		}
	}

	return sg.expandRecurse(ctx, depth)
}
