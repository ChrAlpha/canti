package archive

import "testing"

func TestStepMetaEncodesWhatDiscoveryReads(t *testing.T) {
	exit := -1
	for _, tc := range []struct {
		meta StepMeta
		want string
	}{
		{StepMeta{Step: 7, Execution: "exited", ExitCode: &exit, Output: ArtifactsRoot + "/effect/output", OutputBytes: 12, OutputComplete: true},
			"step: 7\nexecution: exited\nexit_code: -1\nresolved: false\noutput: /canti/artifacts/effect/output\noutput_bytes: 12\noutput_complete: true\n"},
		{StepMeta{Step: 8, Execution: "failed_to_start", Resolved: true, OutputDetail: "resolved\r\nby operator"},
			"step: 8\nexecution: failed_to_start\nresolved: true\noutput: unavailable\noutput_bytes: 0\noutput_complete: false\noutput_detail: resolved  by operator\n"},
	} {
		encoded := tc.meta.Encode()
		if encoded != tc.want {
			t.Fatalf("Encode() = %q, want %q", encoded, tc.want)
		}
		output, bytes, complete, err := decodeOutput([]byte(encoded), tc.meta.Step)
		if err != nil || output != cmpOutput(tc.meta.Output) || bytes != tc.meta.OutputBytes || complete != tc.meta.OutputComplete {
			t.Fatalf("decodeOutput(%q) = %q, %d, %v, %v", encoded, output, bytes, complete, err)
		}
		if _, _, _, err := decodeOutput([]byte(encoded), tc.meta.Step+1); err == nil {
			t.Fatalf("meta of step %d decoded as step %d", tc.meta.Step, tc.meta.Step+1)
		}
	}
}

func cmpOutput(output string) string {
	if output == "" {
		return unavailable
	}
	return output
}
