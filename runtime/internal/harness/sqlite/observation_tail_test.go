package sqlite

import (
	"context"
	"strings"
	"testing"

	"github.com/ChrAlpha/canti/runtime/internal/harness"
)

func seedObservations(t *testing.T, s *Store, n int, distinct bool) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		source, subject := "ci", "build"
		if distinct {
			subject = "build-" + itoa(i)
		}
		if _, err := s.Admit(ctx, harness.InputSubmission{Authority: harness.AuthorityObservation,
			Source: source, Subject: subject, Text: "observation " + itoa(i), Now: admittedAt}); err != nil {
			t.Fatal(err)
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func TestObservationTailIsIndexedAndBounded(t *testing.T) {
	for _, tc := range []struct {
		name     string
		history  int
		distinct bool
		want     int
		wantKeys int
	}{
		{"64 same-key", 64, false, 1, 1},
		{"4096 same-key", 4096, false, 1, 1},
		{"16 distinct", 16, true, 16, 16},
		{"1024 distinct", 1024, true, 16, 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := open(t, tempPath(t))
			seedObservations(t, s, tc.history, tc.distinct)
			got, err := s.Observations(context.Background())
			if err != nil || len(got) != tc.want {
				t.Fatalf("observations = %d, %v; want %d", len(got), err, tc.want)
			}
			var keys int
			if err := s.db.QueryRowContext(context.Background(), `SELECT count(*) FROM observation_keys`).Scan(&keys); err != nil {
				t.Fatal(err)
			}
			if keys != tc.wantKeys {
				t.Fatalf("observation keys = %d, want %d", keys, tc.wantKeys)
			}
			rows, err := s.db.QueryContext(context.Background(), "EXPLAIN QUERY PLAN "+observationsSQL, harness.MaxObservationKeys)
			if err != nil {
				t.Fatal(err)
			}
			defer rows.Close()
			var plan strings.Builder
			for rows.Next() {
				var id, parent, notused int
				var detail string
				if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
					t.Fatal(err)
				}
				plan.WriteString(detail)
				plan.WriteByte('\n')
			}
			if err := rows.Err(); err != nil {
				t.Fatal(err)
			}
			text := plan.String()
			if !strings.Contains(text, "observation_keys") || strings.Contains(text, "SCAN inputs") ||
				strings.Contains(text, "TEMP B-TREE") {
				t.Fatalf("observation tail plan is history-dependent:\n%s", text)
			}
		})
	}
}
