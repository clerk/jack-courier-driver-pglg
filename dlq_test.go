package pglg

import (
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	courier "github.com/clerk/jack-courier-lib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMapResults_ShadowReorderingAndRejections(t *testing.T) {
	createdAt := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)
	hugeErr := strings.Repeat("x", dlqErrorMessageMax+128)

	inserts := []parsedInsert{
		{id: 1, createdAt: createdAt, producerID: "p", jobType: "normal_a"},
		{id: 2, createdAt: createdAt, producerID: "p", jobType: "shadow_a", shadow: true},
		{id: 3, createdAt: createdAt, producerID: "p", jobType: "normal_b"},
		{id: 4, createdAt: createdAt, producerID: "p", jobType: "shadow_b", shadow: true},
	}
	// courier-lib returns normals first, then shadows.
	results := []courier.SubmitResult{
		{CorrelationID: "1", JobID: "j1"},
		{CorrelationID: "3", Err: "boom-3", Reason: "internal_error"},
		{CorrelationID: "2", Err: "bad-2", Reason: "validation_error"},
		{CorrelationID: "4", Err: hugeErr},
	}

	rows, err := mapResults(inserts, results)
	require.NoError(t, err)
	require.Len(t, rows, 3)

	byID := map[int64]dlqRow{}
	for _, r := range rows {
		byID[r.id] = r
	}

	assert.Equal(t, "normal_b", byID[3].jobType)
	assert.Equal(t, "internal_error", byID[3].reason)
	assert.Equal(t, "boom-3", byID[3].errorMessage)
	assert.False(t, byID[3].shadow)

	assert.Equal(t, "shadow_a", byID[2].jobType)
	assert.Equal(t, "validation_error", byID[2].reason)
	assert.True(t, byID[2].shadow)

	assert.Equal(t, "shadow_b", byID[4].jobType)
	assert.Equal(t, dlqReasonUnspecified, byID[4].reason)
	assert.Len(t, byID[4].errorMessage, dlqErrorMessageMax)
}

func TestMapResults_ValidationErrors(t *testing.T) {
	insert := parsedInsert{id: 1, producerID: "p", jobType: "j"}

	tests := []struct {
		name    string
		inserts []parsedInsert
		results []courier.SubmitResult
		errSub  string
	}{
		{
			name:    "length mismatch",
			inserts: []parsedInsert{insert, {id: 2}},
			results: []courier.SubmitResult{{CorrelationID: "1"}},
			errSub:  "1 results for 2 submitted jobs",
		},
		{
			name:    "duplicate insert correlation id",
			inserts: []parsedInsert{{id: 1}, {id: 1}},
			results: []courier.SubmitResult{{CorrelationID: "1"}, {CorrelationID: "1"}},
			errSub:  "duplicate CorrelationID \"1\" among submitted inserts",
		},
		{
			name:    "duplicate result correlation id",
			inserts: []parsedInsert{{id: 1}, {id: 2}},
			results: []courier.SubmitResult{{CorrelationID: "1"}, {CorrelationID: "1"}},
			errSub:  "duplicate CorrelationID \"1\" in submit results",
		},
		{
			name:    "unknown result correlation id",
			inserts: []parsedInsert{{id: 1}, {id: 2}},
			results: []courier.SubmitResult{{CorrelationID: "1"}, {CorrelationID: "99"}},
			errSub:  "unknown CorrelationID \"99\" in submit results",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rows, err := mapResults(tc.inserts, tc.results)
			assert.Nil(t, rows)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.errSub)
		})
	}
}

func TestTruncateProducesValidUTF8(t *testing.T) {
	input := strings.Repeat("a", dlqErrorMessageMax-1) + "€"

	got := truncate(input, dlqErrorMessageMax)

	assert.True(t, utf8.ValidString(got))
	assert.LessOrEqual(t, len(got), dlqErrorMessageMax)
	assert.Equal(t, strings.Repeat("a", dlqErrorMessageMax-1), got)
}
