package docker

import "testing"

func TestLimitsParseAsTheDockerCLIParsesThem(t *testing.T) {
	for in, want := range map[string]int64{
		"0": 0, "1024": 1024, "1.5": 1, "1e3": 1000, "10b": 10, "10B": 10, "1k": 1 << 10, "1kb": 1 << 10, "1KiB": 1 << 10,
		"256m": 256 << 20, "256M": 256 << 20, "256mb": 256 << 20, "2g": 2 << 30, "2 g": 2 << 30, "2 GiB": 2 << 30,
		"1.5g": 3 << 29, "1t": 1 << 40, "1p": 1 << 50,
	} {
		if got, err := ramBytes(in); err != nil || got != want {
			t.Errorf("ramBytes(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "m", "-1m", "1x", "1kk", "1mbb", "1bb", "1 2", "1kib ", "abc", "1e400"} {
		if got, err := ramBytes(in); err == nil {
			t.Errorf("ramBytes(%q) = %d, want an error", in, got)
		}
	}
	for in, want := range map[string]int64{"0.5": 5e8, "1": 1e9, "1.5": 15e8, "1/2": 5e8, "0.000000001": 1, "1e-9": 1, "16": 16e9} {
		if got, err := nanoCPUs(in); err != nil || got != want {
			t.Errorf("nanoCPUs(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "x", "0.0000000001", "1/3", "1,5"} {
		if got, err := nanoCPUs(in); err == nil {
			t.Errorf("nanoCPUs(%q) = %d, want an error", in, got)
		}
	}
}
