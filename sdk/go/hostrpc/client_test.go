package hostrpc

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func answerOnce(t *testing.T, answer Response) (*Client, <-chan Request) {
	t.Helper()
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "host.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	requests := make(chan Request, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var request Request
		if json.NewDecoder(conn).Decode(&request) != nil {
			return
		}
		requests <- request
		_ = json.NewEncoder(conn).Encode(answer)
	}()
	return &Client{Path: listener.Addr().String()}, requests
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestClientMapsWireErrorsWithoutRuntimeTypes(t *testing.T) {
	for _, tc := range []struct {
		code string
		want error
	}{
		{CodeConflict, ErrConflict},
		{CodeClosing, ErrClosing},
		{CodeDriving, ErrDriving},
		{CodeCursor, ErrCursor},
		{CodeProtocol, ErrProtocol},
	} {
		t.Run(tc.code, func(t *testing.T) {
			client, requests := answerOnce(t, Response{Protocol: Protocol, Error: &Error{Code: tc.code, Message: "refused"}})
			_, err := client.Call(testContext(t), Request{Op: OpView})
			if !errors.Is(err, tc.want) {
				t.Fatalf("Call error = %v, want %v", err, tc.want)
			}
			if request := <-requests; request.Protocol != Protocol || request.Op != OpView {
				t.Fatalf("request = %+v", request)
			}
		})
	}
	client, _ := answerOnce(t, Response{Protocol: Protocol, Error: &Error{Code: CodeSpec, Message: "invalid spec"}})
	_, err := client.Validate(testContext(t), Validation{})
	var spec SpecError
	if !errors.As(err, &spec) || spec.Error() != "invalid spec" {
		t.Fatalf("Validate error = %v, want SDK SpecError", err)
	}
}

func TestClientKeepsProgressAndWaitCursorsOpaque(t *testing.T) {
	const runID = "run-qa"
	const cursor = "opaque:/?next=not-a-sequence"
	client, requests := answerOnce(t, Response{Protocol: Protocol, Page: &Page{Next: cursor, More: true}})
	page, err := client.Progress(testContext(t), runID, "", 7)
	if err != nil || page.Next != cursor {
		t.Fatalf("Progress = %+v, %v", page, err)
	}
	if request := <-requests; request.Op != OpProgress || request.RunID != runID || request.Cursor != "" || request.Limit != 7 {
		t.Fatalf("progress request = %+v", request)
	}
	client, requests = answerOnce(t, Response{Protocol: Protocol, Wait: &Wait{Cursor: "after:opaque", Driver: DriverIdle}})
	wait, err := client.Wait(testContext(t), runID, cursor)
	if err != nil || wait.Cursor != "after:opaque" {
		t.Fatalf("Wait = %+v, %v", wait, err)
	}
	if request := <-requests; request.Op != OpWait || request.RunID != runID || request.Cursor != cursor {
		t.Fatalf("wait request = %+v", request)
	}
}

func TestClientValidatesThroughPublicProtocol(t *testing.T) {
	validation := Validation{State: "/state", Submission: Submission{SessionID: "session-qa", Goal: "input"}}
	client, requests := answerOnce(t, Response{Protocol: Protocol, Validated: &Validated{State: "/state", GoalLimit: 4096}})
	got, err := client.Validate(testContext(t), validation)
	if err != nil || got.State != validation.State || got.GoalLimit != 4096 {
		t.Fatalf("Validate = %+v, %v", got, err)
	}
	request := <-requests
	if request.Op != OpValidate || request.Protocol != Protocol || request.Validate == nil || request.Validate.State != validation.State || request.Validate.Submission.Goal != "input" {
		t.Fatalf("validation request = %+v", request)
	}
}

func TestClientDistinguishesNoHostFromLostAnswer(t *testing.T) {
	_, err := (&Client{Path: filepath.Join(t.TempDir(), "missing.sock")}).Hello(testContext(t))
	if !Unavailable(err) || Lost(err) {
		t.Fatalf("missing host error = %v; want unavailable only", err)
	}
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "host.sock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var request Request
		_ = json.NewDecoder(conn).Decode(&request)
	}()
	_, err = (&Client{Path: listener.Addr().String()}).Hello(testContext(t))
	if !Lost(err) || Unavailable(err) {
		t.Fatalf("lost answer error = %v; want lost only", err)
	}
}

func TestClientRejectsInvalidUTF8BeforeDialIncludingNestedFields(t *testing.T) {
	invalid := string([]byte{0xff})
	for _, tc := range []struct {
		name string
		req  Request
	}{
		{"operation", Request{Op: invalid}},
		{"validation state", Request{Op: OpValidate, Validate: &Validation{State: invalid}}},
		{"submission goal", Request{Op: OpSubmit, Submit: &Submission{Goal: invalid}}},
		{"spec workspace", Request{Op: OpSubmit, Submit: &Submission{Spec: Spec{Workspace: invalid}}}},
		{"spec model", Request{Op: OpSubmit, Submit: &Submission{Spec: Spec{Model: invalid}}}},
		{"binding provider", Request{Op: OpSubmit, Binding: &Binding{Provider: invalid}}},
		{"binding key", Request{Op: OpSubmit, Binding: &Binding{OpenAIKey: invalid}}},
		{"resolution note", Request{Op: OpResolve, Resolve: &Resolution{Note: invalid}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &Client{Path: filepath.Join(t.TempDir(), "missing.sock")}
			_, err := client.Call(testContext(t), tc.req)
			if err == nil || !strings.Contains(err.Error(), "valid UTF-8") || Unavailable(err) {
				t.Fatalf("Call = %v; want UTF-8 refusal before dial", err)
			}
		})
	}
}
