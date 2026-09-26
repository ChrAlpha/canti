package core

import "testing"

var benchmarkDecision Decision
var benchmarkState State
var benchmarkID string

func BenchmarkDecode(b *testing.B) {
	inputs := []struct {
		name string
		raw  []byte
	}{
		{"small", []byte(`{"kind":"exec","payload":"go test ./..."}`)},
		{"escaped", []byte(`{"payload":"line\n\u4f60\u597d\t\\","kind":"return"}`)},
		{"duplicate", []byte(`{"kind":"exec","payload":"x","\u006bind":"return"}`)},
	}
	for _, input := range inputs {
		b.Run(input.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				benchmarkDecision, _ = Decode(input.raw)
			}
		})
	}
}

func BenchmarkStateNext(b *testing.B) {
	state, err := NewState("run-1", Limits{MaxSteps: 10, MaxAttempts: 3})
	if err != nil {
		b.Fatal(err)
	}
	b.Run("freeze", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkState, err = state.Next(RequestFrozen{})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
	state.Phase = PhaseResponseSaved
	state.RequestRev = 1
	body := []byte(`{"kind":"exec","payload":"go test ./..."}`)
	b.Run("decode_exec", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			benchmarkState, err = state.Next(ResponseLoaded{Step: 1, Attempt: 1, Body: body})
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkEffectID(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		benchmarkID = effectID("run-1", 12)
	}
}
