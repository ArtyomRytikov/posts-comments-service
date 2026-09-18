package api

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"time"
)

// gqlgen's WebSocket transport serializes writes but does not set a write
// deadline. Bound each underlying write so an unresponsive peer cannot hold
// the transport mutex (and its cleanup goroutines) indefinitely.
type deadlineWriter struct{ http.ResponseWriter }

func (w deadlineWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hijacker, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("response writer does not support hijacking")
	}
	conn, buffered, err := hijacker.Hijack()
	if err != nil {
		return nil, nil, err
	}
	return deadlineConn{conn}, buffered, nil
}

type deadlineConn struct{ net.Conn }

func (c deadlineConn) Write(data []byte) (int, error) {
	if err := c.Conn.SetWriteDeadline(time.Now().Add(operationTimeout)); err != nil {
		return 0, err
	}
	return c.Conn.Write(data)
}
