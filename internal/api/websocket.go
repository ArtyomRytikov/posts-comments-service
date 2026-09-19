package api

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/99designs/gqlgen/graphql"
	"github.com/gorilla/websocket"
	"github.com/vektah/gqlparser/v2/gqlerror"
)

const maxSocketOperations = 32

// socketTransport supplies transport-level bounds and owns cancellation by
// operation ID. gqlgen still performs all GraphQL validation and execution.
// Legacy message names are supported for the bundled Playground.
type socketTransport struct{ logger *slog.Logger }

func (socketTransport) Supports(r *http.Request) bool { return websocket.IsWebSocketUpgrade(r) }

type socketMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

type socketOutput struct {
	ID      string `json:"id,omitempty"`
	Type    string `json:"type"`
	Payload any    `json:"payload,omitempty"`
}

type socketOperation struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type socketConnection struct {
	ctx     context.Context
	cancel  context.CancelFunc
	conn    *websocket.Conn
	exec    graphql.GraphExecutor
	headers http.Header
	logger  *slog.Logger
	legacy  bool
	writeMu sync.Mutex
	mu      sync.Mutex
	active  map[string]*socketOperation
	workers sync.WaitGroup
}

func (t socketTransport) Do(w http.ResponseWriter, r *http.Request, exec graphql.GraphExecutor) {
	upgrader := websocket.Upgrader{HandshakeTimeout: 5 * time.Second,
		Subprotocols: []string{"graphql-transport-ws", "graphql-ws"}}
	// Gorilla's default origin check requires the Origin host to match r.Host.
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	c := &socketConnection{ctx: ctx, cancel: cancel, conn: conn, exec: exec,
		headers: r.Header, logger: t.logger, legacy: conn.Subprotocol() == "graphql-ws", active: make(map[string]*socketOperation)}
	stopClose := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer func() {
		cancel()
		_ = conn.Close()
		stopClose()
		c.workers.Wait()
	}()
	if conn.Subprotocol() == "" {
		c.close(4406, "unsupported WebSocket subprotocol")
		return
	}
	conn.SetReadLimit(maxBodyBytes)
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	initialized := false
	conn.SetPongHandler(func(string) error {
		if initialized {
			return conn.SetReadDeadline(time.Now().Add(time.Minute))
		}
		return nil
	})
	for {
		var msg socketMessage
		if err := conn.ReadJSON(&msg); err != nil {
			if timeout, ok := err.(net.Error); ok && timeout.Timeout() && !initialized {
				c.close(4408, "connection initialization timeout")
			}
			return
		}
		switch msg.Type {
		case "connection_init":
			if initialized {
				c.close(4429, "connection already initialized")
				return
			}
			var payload map[string]json.RawMessage
			if len(msg.Payload) > 0 && json.Unmarshal(msg.Payload, &payload) != nil {
				c.close(4400, "invalid initialization payload")
				return
			}
			initialized = true
			_ = conn.SetReadDeadline(time.Now().Add(time.Minute))
			if !c.send(socketOutput{Type: "connection_ack"}) {
				return
			}
			c.workers.Add(1)
			go c.heartbeat()
		case "ping":
			if !c.send(socketOutput{Type: "pong", Payload: msg.Payload}) {
				return
			}
		case "pong":
			if initialized {
				_ = conn.SetReadDeadline(time.Now().Add(time.Minute))
			}
		case "subscribe", "start":
			if !initialized {
				c.close(4401, "connection is not initialized")
				return
			}
			if (msg.Type == "start") != c.legacy {
				c.close(4400, "unexpected operation message")
				return
			}
			if !c.start(msg) {
				return
			}
		case "complete", "stop":
			if (msg.Type == "stop") != c.legacy {
				c.close(4400, "unexpected operation message")
				return
			}
			c.stop(msg.ID)
		case "connection_terminate":
			if c.legacy {
				return
			}
			fallthrough
		default:
			c.close(4400, "unexpected message")
			return
		}
	}
}

func (c *socketConnection) start(msg socketMessage) bool {
	if msg.ID == "" || len(msg.ID) > 128 {
		c.close(4400, "invalid operation ID")
		return false
	}
	c.mu.Lock()
	_, duplicate := c.active[msg.ID]
	full := len(c.active) >= maxSocketOperations
	if duplicate || full {
		c.mu.Unlock()
		if duplicate {
			c.close(4409, "operation ID is already active")
		} else {
			c.close(websocket.ClosePolicyViolation, "too many active operations")
		}
		return false
	}
	ctx, cancel := context.WithCancel(c.ctx)
	op := &socketOperation{ctx: ctx, cancel: cancel}
	c.active[msg.ID] = op
	c.mu.Unlock()
	c.workers.Add(1)
	go c.execute(msg, op)
	return true
}

func (c *socketConnection) execute(msg socketMessage, op *socketOperation) {
	terminal := socketOutput{ID: msg.ID, Type: "complete"}
	ctx := graphql.StartOperationTrace(op.ctx)
	defer func() {
		if recovered := recover(); recovered != nil {
			c.logger.ErrorContext(ctx, "panic in WebSocket operation", "panic", recovered)
			terminal = socketOutput{ID: msg.ID, Type: "error", Payload: []*gqlerror.Error{{Message: "internal server error", Extensions: map[string]any{"code": "INTERNAL_SERVER_ERROR"}}}}
		}
		c.finish(msg.ID, op, terminal)
		c.workers.Done()
	}()
	var params graphql.RawParams
	decoder := json.NewDecoder(bytes.NewReader(msg.Payload))
	decoder.UseNumber()
	if err := decoder.Decode(&params); err != nil {
		terminal = socketOutput{ID: msg.ID, Type: "error", Payload: []*gqlerror.Error{{Message: "invalid operation payload", Extensions: map[string]any{"code": "BAD_USER_INPUT"}}}}
		return
	}
	params.Headers = c.headers
	params.ReadTime = graphql.TraceTiming{Start: graphql.Now(), End: graphql.Now()}
	opCtx, errs := c.exec.CreateOperationContext(ctx, &params)
	ctx = graphql.WithOperationContext(ctx, opCtx)
	if len(errs) > 0 {
		terminal = socketOutput{ID: msg.ID, Type: "error", Payload: c.exec.DispatchError(ctx, errs).Errors}
		return
	}
	responses, responseCtx := c.exec.DispatchOperation(ctx, opCtx)
	for {
		response := responses(responseCtx)
		if response == nil || op.ctx.Err() != nil {
			return
		}
		kind := "next"
		if c.legacy {
			kind = "data"
		}
		c.writeMu.Lock()
		c.mu.Lock()
		active := c.active[msg.ID] == op && op.ctx.Err() == nil
		c.mu.Unlock()
		ok := active && c.writeLocked(socketOutput{ID: msg.ID, Type: kind, Payload: response})
		c.writeMu.Unlock()
		if !ok {
			return
		}
	}
}

func (c *socketConnection) stop(id string) {
	// Serialize cancellation with writes, preventing an old operation's final
	// response from being sent after its ID has been reused by a new operation.
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	defer c.mu.Unlock()
	if op := c.active[id]; op != nil {
		delete(c.active, id)
		op.cancel()
	}
}

func (c *socketConnection) finish(id string, op *socketOperation, terminal socketOutput) {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	active := c.active[id] == op
	if active {
		delete(c.active, id)
	}
	c.mu.Unlock()
	if active && op.ctx.Err() == nil {
		c.writeLocked(terminal)
	}
	op.cancel()
}

func (c *socketConnection) send(msg socketOutput) bool {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.writeLocked(msg)
}

func (c *socketConnection) writeLocked(msg socketOutput) bool {
	if c.ctx.Err() != nil {
		return false
	}
	_ = c.conn.SetWriteDeadline(time.Now().Add(operationTimeout))
	if err := c.conn.WriteJSON(msg); err != nil {
		c.cancel()
		return false
	}
	return true
}

func (c *socketConnection) heartbeat() {
	defer c.workers.Done()
	ticker := time.NewTicker(20 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			// WebSocket control pings work with both GraphQL subprotocols.
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(operationTimeout)); err != nil {
				c.cancel()
				return
			}
		}
	}
}

func (c *socketConnection) close(code int, reason string) {
	_ = c.conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(time.Second))
	c.cancel()
}
