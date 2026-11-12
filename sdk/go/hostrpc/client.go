package hostrpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"regexp"
	"syscall"
	"time"
)

// Client calls the host listening on the socket at Path. Each call is one
// connection; a call keeps its request, so a caller can send it again after
// a connection that broke before its answer.
type Client struct{ Path string }

// RemoteError is an operation the host refused or failed. Its protocol-owned code
// and sentinel errors require no runtime implementation.
type RemoteError struct{ Code, Message string }

func (e *RemoteError) Error() string { return e.Message }

// Is reports the protocol errors that the code of e stands for.
func (e *RemoteError) Is(target error) bool {
	switch e.Code {
	case CodeConflict:
		return target == ErrConflict
	case CodeHeld:
		return target == ErrHeld
	case CodeClosing:
		return target == ErrClosing
	case CodeDriving:
		return target == ErrDriving
	case CodeCursor:
		return target == ErrCursor
	}
	return false
}

// ErrProtocol reports a host of another protocol version.
var ErrProtocol = errors.New("hostrpc: the host speaks another protocol version")

// Unavailable reports a call that reached no host: there is no socket, or no
// process listens on it, so a host may be started.
func Unavailable(err error) bool {
	return errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED)
}

// Lost reports a call whose connection broke after its request may have
// arrived, so that its outcome is unknown and the request may be sent again.
func Lost(err error) bool {
	var oe *net.OpError
	return errors.As(err, &oe) && oe.Op != "dial" || errors.Is(err, errNoAnswer)
}

var errNoAnswer = errors.New("hostrpc: the connection closed before the host answered")

// Call sends req and returns the host's response, or the host's error.
func (c *Client) Call(ctx context.Context, req Request) (Response, error) {
	req.Protocol = Protocol
	if err := req.CheckText(); err != nil {
		return Response{}, err
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", c.Path)
	if err != nil {
		return Response{}, err
	}
	defer conn.Close()
	if err := samePeer(conn); err != nil {
		return Response{}, err
	}
	stop := context.AfterFunc(ctx, func() { conn.SetDeadline(time.Now()) })
	defer stop()
	e := json.NewEncoder(conn)
	e.SetEscapeHTML(false)
	if err := e.Encode(req); err != nil {
		return Response{}, err
	}
	var resp Response
	if err := json.NewDecoder(io.LimitReader(conn, maxResponse)).Decode(&resp); err != nil {
		if ctx.Err() != nil {
			return Response{}, ctx.Err()
		}
		return Response{}, fmt.Errorf("%w: %v", errNoAnswer, err)
	}
	if resp.Protocol != Protocol || resp.Error != nil && resp.Error.Code == CodeProtocol {
		return resp, protocolError(resp)
	}
	if e := resp.Error; e != nil {
		if e.Code == CodeSpec {
			return resp, SpecError{Err: errors.New(e.Message)}
		}
		return resp, &RemoteError{Code: e.Code, Message: e.Message}
	}
	return resp, nil
}

var hostPID = regexp.MustCompile(`\bpid (\d+)\b`)

// protocolError reports a host of another protocol version, whose answer is
// resp, with how to replace it: never automatically, since it may drive a
// run, but with the canti that started it, or with SIGTERM to its process,
// which ends a host of any version gracefully; this client's own host -stop
// is refused by it.
func protocolError(resp Response) error {
	var said string
	if e := resp.Error; e != nil {
		said = e.Message
	}
	process, remedy := "", "stop it with the canti that started it, or end its process with SIGTERM once it drives nothing"
	if m := hostPID.FindStringSubmatch(said); m != nil {
		process = ", process " + m[1] + ","
		remedy = "stop it with the canti that started it, or with \"kill -TERM " + m[1] + "\" once it drives nothing"
	}
	return fmt.Errorf("%w: the host of this state directory%s speaks host protocol %d and this canti speaks %d; it is never replaced while it may drive a run: %s",
		ErrProtocol, process, resp.Protocol, Protocol, remedy)
}

// Hello describes the host.
func (c *Client) Hello(ctx context.Context) (Hello, error) {
	resp, err := c.Call(ctx, Request{Op: OpHello})
	if err != nil {
		return Hello{}, err
	}
	if resp.Hello == nil {
		return Hello{}, errors.New("hostrpc: hello without an answer")
	}
	return *resp.Hello, nil
}

func (c *Client) run(ctx context.Context, req Request) (Run, error) {
	resp, err := c.Call(ctx, req)
	var r Run
	if resp.Run != nil {
		r = *resp.Run
	}
	if err == nil && resp.Run == nil {
		err = fmt.Errorf("hostrpc: %s without a run", req.Op)
	}
	return r, err
}

// Submit admits one input. When it opened a run, the host drives it with the
// binding unless the input was already admitted.
func (c *Client) Submit(ctx context.Context, s Submission, b Binding, once bool) (Ack, error) {
	resp, err := c.Call(ctx, Request{Op: OpSubmit, Submit: &s, Binding: &b, Once: once})
	if err == nil && resp.Ack == nil {
		err = errors.New("hostrpc: submit without an acknowledgement")
	}
	if err != nil {
		return Ack{}, err
	}
	return *resp.Ack, nil
}

// Drive has the host drive run id with the binding.
func (c *Client) Drive(ctx context.Context, id string, b Binding, once bool) (Run, error) {
	return c.run(ctx, Request{Op: OpDrive, RunID: id, Binding: &b, Once: once})
}

// View returns run id, or the latest run when id is empty.
func (c *Client) View(ctx context.Context, id string) (Run, error) {
	return c.run(ctx, Request{Op: OpView, RunID: id})
}

// Stop stops run id; the host bounds its cleanup by cleanup.
func (c *Client) Stop(ctx context.Context, id string, cleanup time.Duration) (Run, error) {
	return c.run(ctx, Request{Op: OpStop, RunID: id, CleanupMS: cleanup.Milliseconds()})
}

// Resolve records r for the unknown effect of run id.
func (c *Client) Resolve(ctx context.Context, id string, r Resolution) (Run, error) {
	return c.run(ctx, Request{Op: OpResolve, RunID: id, Resolve: &r})
}

// Inherited returns the constraints a new non-fresh input inherits: those of
// the latest run.
func (c *Client) Inherited(ctx context.Context) (string, error) {
	resp, err := c.Call(ctx, Request{Op: OpInherited})
	if err != nil || resp.Constraints == nil {
		return "", err
	}
	return *resp.Constraints, nil
}

// Observe admits evidence from source about subject. It never starts a run.
func (c *Client) Observe(ctx context.Context, o Observation) (Ack, error) {
	resp, err := c.Call(ctx, Request{Op: OpObserve, Observe: &o})
	if err == nil && resp.Ack == nil {
		err = errors.New("hostrpc: observe without an acknowledgement")
	}
	if err != nil {
		return Ack{}, err
	}
	return *resp.Ack, nil
}

// Withdraw takes back the named held input, or every held input when inputID
// is empty, and returns them in position order.
func (c *Client) Withdraw(ctx context.Context, inputID string) ([]Input, error) {
	resp, err := c.Call(ctx, Request{Op: OpWithdraw, Withdraw: &Withdrawal{InputID: inputID}})
	if err != nil {
		return nil, err
	}
	return resp.Withdrawn, nil
}

// CancelEffect cancels the accepted effect of run id without stopping the run.
func (c *Client) CancelEffect(ctx context.Context, id string) (Run, error) {
	return c.run(ctx, Request{Op: OpCancel, RunID: id})
}

// ShutdownHost asks the host to shut down.
func (c *Client) ShutdownHost(ctx context.Context) error {
	_, err := c.Call(ctx, Request{Op: OpShutdown})
	return err
}

// Progress reads the agent log after the opaque cursor, or from its start
// when empty.
func (c *Client) Progress(ctx context.Context, after string, limit int) (Page, error) {
	resp, err := c.Call(ctx, Request{Op: OpProgress, Cursor: after, Limit: limit})
	if err != nil {
		return Page{}, err
	}
	if resp.Page == nil {
		return Page{}, errors.New("hostrpc: progress without a page")
	}
	return *resp.Page, nil
}

// Wait waits for the agent log to advance beyond the opaque cursor, or for
// the agent to become quiescent.
func (c *Client) Wait(ctx context.Context, after string) (Wait, error) {
	resp, err := c.Call(ctx, Request{Op: OpWait, Cursor: after, TimeoutMS: maxWait.Milliseconds()})
	if err != nil {
		return Wait{}, err
	}
	if resp.Wait == nil {
		return Wait{}, errors.New("hostrpc: wait without an answer")
	}
	return *resp.Wait, nil
}
