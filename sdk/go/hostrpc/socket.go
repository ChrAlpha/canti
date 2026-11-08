package hostrpc

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// SocketName is the socket of a host in its state directory, beside the lock
// that admits one host per state directory.
const SocketName = "host.sock"

// maxPath bounds the path of a Unix socket on Linux, without its NUL.
const maxPath = 107

// SocketPath returns the socket path of the host of state, or an error when
// the path is too long for a Unix socket.
func SocketPath(state string) (string, error) {
	p := filepath.Join(state, SocketName)
	if len(p) > maxPath {
		return p, fmt.Errorf("the host socket %s is %d bytes, more than the %d a Unix socket allows; use a shorter -state", p, len(p), maxPath)
	}
	return p, nil
}

func samePeer(conn net.Conn) error {
	sc, ok := conn.(syscall.Conn)
	if !ok {
		return errors.New("hostrpc: not a Unix connection")
	}
	raw, err := sc.SyscallConn()
	if err != nil {
		return err
	}
	var cred *unix.Ucred
	var credErr error
	if err := raw.Control(func(fd uintptr) { cred, credErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
		return err
	}
	if credErr != nil {
		return credErr
	}
	if int(cred.Uid) != os.Getuid() {
		return fmt.Errorf("hostrpc: peer uid %d is not %d", cred.Uid, os.Getuid())
	}
	return nil
}
