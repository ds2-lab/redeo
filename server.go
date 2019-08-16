package redeo

import (
	"github.com/wangaoone/redeo/resp"
	"net"
	"strings"
	"sync"
	"time"
)

const (
	ASYNC_CB_NAME = "callback"
)

// Server configuration
type Server struct {
	config *Config
	info   *ServerInfo

	cmds     map[string]interface{}
	mu       sync.RWMutex
	released *sync.WaitGroup
}

// NewServer creates a new server instance
func NewServer(config *Config) *Server {
	if config == nil {
		config = new(Config)
	}

	return &Server{
		config: config,
		info:   newServerInfo(),
		cmds:   make(map[string]interface{}),
	}
}

// Info returns the server info registry
func (srv *Server) Info() *ServerInfo { return srv.info }

// Handle registers a handler for a command.
func (srv *Server) Handle(name string, h Handler) {
	srv.mu.Lock()
	srv.cmds[strings.ToLower(name)] = h
	srv.mu.Unlock()
}

// HandleFunc registers a handler func for a command.
func (srv *Server) HandleFunc(name string, fn HandlerFunc) {
	srv.Handle(name, fn)
}

// HandleStream registers a handler for a streaming command.
func (srv *Server) HandleStream(name string, h StreamHandler) {
	srv.mu.Lock()
	srv.cmds[strings.ToLower(name)] = h
	srv.mu.Unlock()
}

// HandleStreamFunc registers a handler func for a command
func (srv *Server) HandleStreamFunc(name string, fn StreamHandlerFunc) {
	srv.HandleStream(name, fn)
}

// Callback registers a handler for a callback command.
func (srv *Server) HandleCallback(cb Callback) {
	srv.mu.Lock()
	srv.cmds[ASYNC_CB_NAME] = cb
	srv.mu.Unlock()
}

// CallbackFunc registers a handler func for a command
func (srv *Server) HandleCallbackFunc(cbf CallbackFunc) {
	srv.HandleCallback(cbf)
}

// Serve accepts incoming connections on a listener, creating a
// new service goroutine for each.
func (srv *Server) Serve(lis net.Listener) error {
	return srv.serveImpl(lis, true)
}

func (srv *Server) ServeAsync(lis net.Listener) error {
	return srv.serveImpl(lis, false)
}

func (srv *Server) serveImpl(lis net.Listener, sync bool) error {
	for {
		cn, err := lis.Accept()
		if err != nil {
			return err
		}

		if ka := srv.config.TCPKeepAlive; ka > 0 {
			if tc, ok := cn.(*net.TCPConn); ok {
				tc.SetKeepAlive(true)
				tc.SetKeepAlivePeriod(ka)
			}
		}

		go srv.serveClient(newClient(cn), sync)
	}
}

func (srv *Server) GetClient(id uint64) (*Client, bool) {
	if cInfo, ok := srv.info.clients.Client(id); ok {
		return cInfo.Client, ok
	} else {
		return nil, ok
	}
}

func (srv *Server) Close(lis net.Listener) {
	lis.Close()
}

func (srv *Server) Release() {
	infos := srv.info.clients.All()
	srv.released = &sync.WaitGroup{}
	for i := 0; i < len(infos); i++ {
		srv.released.Add(1)
		infos[i].Client.Close()
		infos[i].Client = nil
	}
	srv.released.Wait()
	srv.released = nil
}

func (srv *Server) register(c *Client) {
	srv.info.register(c)
}

func (srv *Server) deregister(clientID uint64) {
	srv.info.deregister(clientID)
	if srv.released != nil {
		srv.released.Done()
	}
}

// Starts a new session, serving client
func (srv *Server) serveClient(c *Client, sync bool) error {
	// Release client on exit
	defer c.release()

	// Register client
	srv.register(c)
	defer srv.deregister(c.id)

	if !sync {
		go srv.handleResponses(c)
	}
	return srv.handleRequests(c)
}

func (srv *Server) handleRequests(c *Client) error {
	// Create perform callback
	perform := func(name string) error {
		return srv.perform(c, name)
	}
	// Init request/response loop
	for !c.closed {
		// set deadline
		if d := srv.config.Timeout; d > 0 {
			c.cn.SetDeadline(time.Now().Add(d))
		}

		// perform pipeline
		if err := c.pipeline(perform); err != nil {
			c.wr.AppendError("ERR " + err.Error())

			if !resp.IsProtocolError(err) {
				_ = c.wr.Flush()
				return err
			}
		}

		// flush buffer, return on errors
		if err := c.wr.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (srv *Server) handleResponses(c *Client) (err error) {
	for {
		select {
		case <-c.done:
			return nil
		case response := <-c.responses:
			// find handler
			srv.mu.RLock()
			cb, ok := srv.cmds[ASYNC_CB_NAME]
			srv.mu.RUnlock()

			if !ok {
				c.wr.AppendError(UnknownCommand(ASYNC_CB_NAME))
				err = c.wr.Flush()
			}

			cb.(Callback).ServeCallback(c.wr, response)

			// flush buffer, return on errors
			if err := c.wr.Flush(); err != nil {
				return err
			}
		}
	}
}

func (srv *Server) perform(c *Client, name string) (err error) {
	norm := strings.ToLower(name)

	// find handler
	srv.mu.RLock()
	h, ok := srv.cmds[norm]
	srv.mu.RUnlock()

	if !ok {
		c.wr.AppendError(UnknownCommand(name))
		_ = c.rd.SkipCmd()
		return
	}

	// register call
	srv.info.command(c.id, norm)

	switch handler := h.(type) {
	case Handler:
		if c.cmd, err = c.readCmd(c.cmd); err != nil {
			return
		}

		handler.ServeRedeo(c.wr, c.cmd)

	case StreamHandler:
		if c.scmd, err = c.streamCmd(c.scmd); err != nil {
			return
		}
		defer c.scmd.Discard()

		handler.ServeRedeoStream(c.wr, c.scmd)
	}

	// flush when buffer is large enough
	if n := c.wr.Buffered(); n > resp.MaxBufferSize/2 {
		err = c.wr.Flush()
	}
	return
}

/*
 * Lambda store part
 */

// Lambda facing serve client
func (srv *Server) ServeForeignClient(cn net.Conn) error {
	return srv.serveClient(newClient(cn), true)
}
