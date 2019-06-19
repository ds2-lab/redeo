package redeo

import (
	"fmt"
	"github.com/bsm/redeo/resp"
	"net"
	"strings"
	"sync"
	"time"
)

// Server configuration
type Server struct {
	config *Config
	info   *ServerInfo

	cmds map[string]interface{}
	mu   sync.RWMutex
}

type Req struct {
	Cmd      string
	Argument *resp.Command
	Cid      int
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

// Serve accepts incoming connections on a listener, creating a
// new service goroutine for each.
func (srv *Server) Serve(lis net.Listener) error {
	for {
		cn, err := lis.Accept()
		if err != nil {
			return err
		}
		fmt.Println("Accept", cn.RemoteAddr())

		if ka := srv.config.TCPKeepAlive; ka > 0 {
			if tc, ok := cn.(*net.TCPConn); ok {
				tc.SetKeepAlive(true)
				tc.SetKeepAlivePeriod(ka)
			}
		}

		go srv.serveClient(newClient(cn))
	}
}

func (srv *Server) serveClient(c *Client) {
	// Release client on exit
	defer c.release()

	// Register client
	srv.info.register(c)
	defer srv.info.deregister(c.id)

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
				return
			}
		}

		// flush buffer, return on errors
		if err := c.wr.Flush(); err != nil {
			return
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

//func (srv *Server) myPeekCmd(c *Client, channel chan string) error {
//	for more := true; more; more = c.rd.Buffered() != 0 {
//		fmt.Println("Peeking cmd")
//		name, err := c.rd.PeekCmd()
//		if err != nil {
//			_ = c.rd.SkipCmd()
//			fmt.Println("peeking err is ", err)
//		}
//		norm := strings.ToLower(name)
//		// register call
//		srv.info.command(c.id, norm)
//
//		if c.cmd, err = c.readCmd(c.cmd); err != nil {
//			fmt.Println("read cmd err", err)
//		}
//
//		fmt.Println("cmd name is ", name)
//		channel <- name
//		// flush when buffer is large enough
//		if n := c.wr.Buffered(); n > resp.MaxBufferSize/2 {
//			err = c.wr.Flush()
//		}
//	}
//	return nil
//}

// new serve with channel initial
func (srv *Server) MyServe(lis net.Listener, cMap map[int]chan interface{}, lambdaChannel chan Req) error {
	// start counter to record client id, initial with 0
	id := 0
	for {
		cn, err := lis.Accept()
		if err != nil {
			return err
		}
		fmt.Println("Accept", cn.RemoteAddr())

		if ka := srv.config.TCPKeepAlive; ka > 0 {
			if tc, ok := cn.(*net.TCPConn); ok {
				tc.SetKeepAlive(true)
				tc.SetKeepAlivePeriod(ka)
			}
		}

		// make channel for every new client
		c := make(chan interface{}, 1)
		// store the new client channel to the channel map
		cMap[id] = c
		go srv.myServeClient(newClient(cn), c, id, lambdaChannel)
		// id increment by 1
		id = id + 1
	}
}

//event handler
//func (srv *Server) myServeClient(c *Client, clientChannel chan interface{}, id int, lambdaChannel chan Req) {
//	fmt.Println("channel id is ", id)
//	cmdChannel := make(chan string, 1)
//
//	// Release client on exit
//	defer c.release()
//
//	// Register client
//	srv.info.register(c)
//	defer srv.info.deregister(c.id)
//
//	// Init request/response loop
//	for !c.closed {
//		// set deadline
//		if d := srv.config.Timeout; d > 0 {
//			c.cn.SetDeadline(time.Now().Add(d))
//		}
//		go srv.myPeekCmd(c, cmdChannel)
//
//		//go func() {
//		select {
//		case cmd := <-cmdChannel:
//			// construct new request
//			newReq := Req{cmd, c.cmd, id}
//			//fmt.Println("command from server:", c.cmd)
//			fmt.Println("newReq is ", newReq)
//			// send new request to lambda channel
//			lambdaChannel <- newReq
//		case b := <-clientChannel:
//			fmt.Println("final response is ", b)
//			c.wr.AppendInt(1)
//			// flush buffer, return on errors
//			if err := c.wr.Flush(); err != nil {
//				fmt.Println("flush err2 is ", err)
//				return
//			}
//		}
//		//}()
//
//	}
//}

func (srv *Server) myServeClient(c *Client, clientChannel chan interface{}, id int, lambdaChannel chan Req) {
	// make helper channel for every client
	helper := make(chan string, 1024*1024)

	// Release client on exit
	defer c.release()
	defer close(helper)

	// Register client
	srv.info.register(c)
	defer srv.info.deregister(c.id)

	// Create perform callback
	perform := func(name string) error {
		return srv.myPerform(c, name)
	}
	// go routine peeking cmd
	go myPeekCmd(c, perform, helper)

	// Init request/response loop
	for !c.closed {
		// set deadline
		if d := srv.config.Timeout; d > 0 {
			c.cn.SetDeadline(time.Now().Add(d))
		}

		select {
		case cmd := <-helper: /* blocking on helper channel while peeking cmd*/
			// construct new request
			newReq := Req{cmd, c.cmd, id}
			//fmt.Println("newReq is ", newReq)
			// send new request to lambda channel
			lambdaChannel <- newReq
		case result := <-clientChannel: /*blocking on receive final result from lambda store*/
			//c.wr.AppendBulkString(result.(string))
			fmt.Println("final response is ", result)
			c.wr.AppendInt(1)
			// flush buffer, return on errors
			if err := c.wr.Flush(); err != nil {
				return
			}
		}

		// flush buffer, return on errors
		if err := c.wr.Flush(); err != nil {
			return
		}
	}
}

// PeekCmd
func myPeekCmd(c *Client, fn func(string) error, channel chan string) {
	for {
		if err := c.peek(fn, channel); err != nil {
			c.wr.AppendError("ERR " + err.Error())
			if !resp.IsProtocolError(err) {
				fmt.Println("flush err", err)
				_ = c.wr.Flush()
				return
			}
		}
	}
}

func (srv *Server) myPerform(c *Client, name string) (err error) {
	norm := strings.ToLower(name)

	// register call
	srv.info.command(c.id, norm)

	if c.cmd, err = c.readCmd(c.cmd); err != nil {
		return
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

// Accept conn
func (srv *Server) Accept(lis net.Listener) net.Conn {
	cn, err := lis.Accept()
	if err != nil {
		fmt.Println(err)
	}
	return cn
}

// Lambda facing serve client
func (srv *Server) Serve_client(cn net.Conn) {
	//for {
	//	go srv.serveClient(newClient(cn))
	//}
	fmt.Println("in the lambda server")
	srv.serveClient(newClient(cn))

}
