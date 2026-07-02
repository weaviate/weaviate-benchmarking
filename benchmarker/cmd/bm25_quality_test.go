package cmd

import (
	"testing"
	"time"
)

// TestSettleMetric verifies the settle state machine returns the per-metric maxima
// and, crucially, does not freeze Recall when NDCG saturates first (the P0 bug).
func TestSettleMetric(t *testing.T) {
	// NDCG plateaus at 0.30 by poll 2 while Recall keeps climbing to 0.80.
	seq := []Results{
		{NDCG: 0.10, Recall: 0.20},
		{NDCG: 0.30, Recall: 0.40},
		{NDCG: 0.30, Recall: 0.55}, // NDCG saturated, recall improving
		{NDCG: 0.30, Recall: 0.70}, // recall improving
		{NDCG: 0.30, Recall: 0.80}, // recall improving
		{NDCG: 0.30, Recall: 0.80}, // stale 1
		{NDCG: 0.30, Recall: 0.80}, // stale 2
		{NDCG: 0.30, Recall: 0.80}, // stale 3 -> settled
		{NDCG: 0.30, Recall: 0.80},
	}
	i := 0
	poll := func() Results {
		r := seq[i]
		if i < len(seq)-1 {
			i++
		}
		return r
	}
	got := settleMetric(poll, time.Millisecond, 3, 0.003, 10*time.Second)
	if got.NDCG != 0.30 {
		t.Errorf("settled NDCG = %.3f, want 0.30", got.NDCG)
	}
	if got.Recall != 0.80 {
		t.Errorf("settled Recall = %.3f, want 0.80 (must not freeze when NDCG saturates first)", got.Recall)
	}
}

func TestSettleMetricStableImmediately(t *testing.T) {
	// A settled index returns identical polls; should settle after `patience` polls.
	poll := func() Results { return Results{NDCG: 0.42, Recall: 0.63} }
	got := settleMetric(poll, time.Millisecond, 3, 0.003, 10*time.Second)
	if got.NDCG != 0.42 || got.Recall != 0.63 {
		t.Errorf("got NDCG=%.3f Recall=%.3f, want 0.42/0.63", got.NDCG, got.Recall)
	}
}
