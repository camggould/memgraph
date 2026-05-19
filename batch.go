package memgraph

import (
	"context"
	"fmt"
	"sort"

	"golang.org/x/sync/errgroup"
)

// BatchSearch fans the given queries out against store in parallel, merges
// results via Reciprocal Rank Fusion (k = SearchBatchRRFK), dedupes by
// LineageID, and truncates to totalLimit. The caller (typically an MCP or
// REST handler) supplies the variants — BatchSearch never invokes an LLM.
//
// Semantics:
//   - queries must have between 1 and SearchBatchMaxQueries entries.
//   - Each query's Limit is clamped to [1, SearchBatchMaxPerQueryLimit] with a
//     default of SearchBatchDefaultPerQueryLimit when ≤ 0.
//   - totalLimit is clamped to [1, SearchBatchMaxTotalLimit] with a default of
//     SearchBatchDefaultTotalLimit when ≤ 0.
//   - Score(d) = sum over each query i where d appears at 1-based rank r:
//     1 / (SearchBatchRRFK + r).
//   - Ties broken by first-appearance order (deterministic across runs).
//
// Errors from any underlying Search call abort the batch and propagate.
func BatchSearch(ctx context.Context, store Store, graphID GraphID, queries []SearchQuery, totalLimit int) (SearchBatchResult, error) {
	if graphID == "" {
		return SearchBatchResult{}, fmt.Errorf("%w: graph_id required", ErrInvalidInput)
	}
	if len(queries) == 0 {
		return SearchBatchResult{}, fmt.Errorf("%w: queries must contain at least 1 query", ErrInvalidInput)
	}
	if len(queries) > SearchBatchMaxQueries {
		return SearchBatchResult{}, fmt.Errorf("%w: queries must contain at most %d entries", ErrInvalidInput, SearchBatchMaxQueries)
	}
	tl := clampLimit(totalLimit, SearchBatchDefaultTotalLimit, SearchBatchMaxTotalLimit)

	perQuery := make([][]SearchHit, len(queries))
	g, gctx := errgroup.WithContext(ctx)
	for i := range queries {
		i := i
		q := queries[i]
		g.Go(func() error {
			hits, err := store.Search(gctx, graphID, SearchQuery{
				Text:      q.Text,
				Kinds:     q.Kinds,
				Tags:      q.Tags,
				FreshOnly: q.FreshOnly,
				Limit:     clampLimit(q.Limit, SearchBatchDefaultPerQueryLimit, SearchBatchMaxPerQueryLimit),
			})
			if err != nil {
				return err
			}
			perQuery[i] = hits
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return SearchBatchResult{}, err
	}

	type acc struct {
		score   float64
		queries []int
		node    Node
	}
	agg := make(map[LineageID]*acc)
	order := make([]LineageID, 0)
	perQueryHits := make([]int, len(queries))
	for qi, hits := range perQuery {
		perQueryHits[qi] = len(hits)
		for r, h := range hits {
			a, ok := agg[h.Node.LineageID]
			if !ok {
				a = &acc{node: h.Node}
				agg[h.Node.LineageID] = a
				order = append(order, h.Node.LineageID)
			}
			a.score += 1.0 / (SearchBatchRRFK + float64(r+1))
			a.queries = append(a.queries, qi)
		}
	}

	merged := make([]SearchBatchHit, 0, len(order))
	for _, lid := range order {
		a := agg[lid]
		merged = append(merged, SearchBatchHit{
			Node:           a.node,
			RRFScore:       a.score,
			QueriesMatched: a.queries,
		})
	}
	// Stable sort: ties keep first-appearance order from `order`.
	sort.SliceStable(merged, func(i, j int) bool {
		return merged[i].RRFScore > merged[j].RRFScore
	})

	unique := len(merged)
	if len(merged) > tl {
		merged = merged[:tl]
	}

	return SearchBatchResult{
		Hits:         merged,
		QueryCount:   len(queries),
		UniqueHits:   unique,
		PerQueryHits: perQueryHits,
	}, nil
}

func clampLimit(req, def, max int) int {
	if req <= 0 {
		return def
	}
	if req > max {
		return max
	}
	return req
}
