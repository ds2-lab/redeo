package redeo

import (
	"fmt"
	"github.com/ScottMansfield/nanolog"
	"github.com/cornelk/hashmap"
	"github.com/wangaoone/redeo/resp"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	LambdaMem int = 3000
)

var metaMap = hashmap.New(1024 * 1024)
var ReqMap = hashmap.New(1024 * 1024)

// Server configuration
type Server struct {
	config *Config
	info   *ServerInfo

	cmds map[string]interface{}
	mu   sync.RWMutex
}

type ClientReqCounter struct {
	Cmd          string
	DataShards   int
	ParityShards int
	Counter      int32
}
type Chunk struct {
	Cmd     string
	ChunkId int64
	ReqId   string
	Body    []byte
}

type Id struct {
	ConnId  int
	ReqId   string
	ChunkId int64
}

type ServerReq struct {
	Id   Id
	Cmd  string
	Key  []byte
	Body []byte
}

type Response struct {
	Cmd  string
	Id   Id
	Key  string
	Body []byte
}

type Group struct {
	Arr        []LambdaInstance
	MemCounter uint64
}

type LambdaInstance struct {
	Name      string
	Id        int
	Alive     bool
	Cn        net.Conn
	W         *resp.RequestWriter
	R         resp.ResponseReader
	C         chan *ServerReq
	AliveLock sync.Mutex
	Counter   uint64
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

// new serve with channel initial，creating a
// new service goroutine for each.
func (srv *Server) MyServe(lis net.Listener, cMap map[int]chan interface{}, group Group, file string, logger func(handle nanolog.Handle, args ...interface{}) error, done chan struct{}) error {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGTERM|syscall.SIGINT|syscall.SIGKILL)
	// start counter to record client id, initial with 0
	connId := 0
	for {
		select {
		case <-sig:
			close(done)
			os.Remove(file)
			return nil
		default:
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
			c := make(chan interface{}, 1024*1024)
			// store the new client channel to the channel map
			cMap[connId] = c
			go srv.MyServeClient(newClient(cn), c, connId, group, logger)
			// id increment by 1
			connId = connId + 1
		}

	}
}

// client handler
func (srv *Server) MyServeClient(c *Client, clientChannel chan interface{}, connId int, group Group, logger func(handle nanolog.Handle, args ...interface{}) error) {
	// make helper channel for every client

	// Release client on exit
	defer c.release()
	// close client and helper channel
	//defer close(helper)
	//defer close(clientChannel)

	// Register client
	srv.info.register(c)
	defer srv.info.deregister(c.id)

	helper := make(chan string, 1024*1024)
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
		//
		/* blocking on helper channel while peeking cmd*/
		//
		case cmd := <-helper:
			// receive request from client
			key := c.cmd.Arg(0)
			switch strings.ToLower(cmd) {
			case "set":
				chunkId, _ := c.cmd.Arg(1).Int()
				lambdaId, _ := c.cmd.Arg(2).Int()
				reqId := c.cmd.Arg(3).String()
				dataShards, _ := c.cmd.Arg(4).Int()
				parityShards, _ := c.cmd.Arg(5).Int()
				val := c.cmd.Arg(6)
				if err := logger(resp.LogStart, "set", reqId, chunkId, time.Now().UnixNano()); err != nil {
					fmt.Println("log start err is ", err)
				}
				//
				ReqMap.GetOrInsert(reqId, &ClientReqCounter{"set", int(dataShards), int(parityShards), 0})
				// check if the key is existed
				// key is "key"+"chunkId"
				lambdaDestination, ok := metaMap.Get(key.String() + strconv.FormatInt(chunkId, 10))
				if ok == false {
					// send shard to the corresponding lambda instance in group
					newReq := ServerReq{Id{connId, reqId, chunkId}, cmd, key, val}
					// send new request to lambda channel
					group.Arr[lambdaId].C <- &newReq
					metaMap.Set(key.String()+strconv.FormatInt(chunkId, 10), lambdaId)
					//resp.MyPrint("KEY is", key.String(), "IN SET, reqId is", reqId, "connId is", connId, "chunkId is", chunkId, "lambdaStore Id is", lambdaId)
					//if err := nanolog.Log(resp.LogServer, key.String(), "SET", reqId, connId, chunkId, lambdaId); err != nil {
					//	fmt.Println("LogServer err", err)
					//	return
					//}
				} else {
					// update the existed key
					newServerReq := ServerReq{Id{connId, reqId, chunkId}, cmd, key, val}
					group.Arr[lambdaDestination.(int64)].C <- &newServerReq
					//resp.MyPrint("KEY is", key.String(), "IN SET UPDATE, reqId is", reqId, "connId is", connId, "chunkId is", chunkId, "lambdaStore Id is", lambdaId)
					//if err := nanolog.Log(resp.LogServer, key.String(), "SET UPDATE", reqId, connId, chunkId, lambdaId); err != nil {
					//	fmt.Println("LogServer err", err)
					//	return
					//}
				}
			case "get":
				chunkId, _ := c.cmd.Arg(1).Int()
				reqId := c.cmd.Arg(2).String()
				dataShards, _ := c.cmd.Arg(3).Int()
				parityShards, _ := c.cmd.Arg(4).Int()
				if err := logger(resp.LogStart, "get", reqId, chunkId, time.Now().UnixNano()); err != nil {
					fmt.Println("log start err is ", err)
				}
				//
				ReqMap.GetOrInsert(reqId, &ClientReqCounter{"get", int(dataShards), int(parityShards), 0})
				// key is "key"+"chunkId"
				lambdaDestination, ok := metaMap.Get(key.String() + strconv.FormatInt(chunkId, 10))
				//resp.MyPrint("KEY is", key.String(), "IN GET, reqId is", reqId, "connId is", connId, "chunkId is", chunkId, "lambdaStore Id is", lambdaDestination)
				//if err := nanolog.Log(resp.LogServer, key.String(), "GET", reqId, connId, chunkId, lambdaDestination.(int64)); err != nil {
				//	fmt.Println("LogServer err", err)
				//	return
				//}
				if ok == false {
					resp.MyPrint("KEY is", key.String(), "not found key in lambda store, please set first")
				}
				newServerReq := ServerReq{Id{ConnId: connId, ReqId: reqId}, cmd, key, nil}
				// send new request to lambda channel
				group.Arr[lambdaDestination.(int64)].C <- &newServerReq
			}

			//
			/* blocking on receive final result from lambda store*/
			//
		case result := <-clientChannel:
			t := time.Now()
			temp := result.(*Chunk)

			var time1 time.Duration
			if temp.Body == nil {
				c.wr.AppendInt(int64(-1))
			} else {
				// chunk Id
				c.wr.AppendInt(int64(temp.ChunkId))
				// chunk Body
				t1 := time.Now()
				c.wr.AppendBulk(temp.Body[0:len(temp.Body)])
				time1 = time.Since(t1)
			}

			t2 := time.Now()
			// flush buffer, return on errors
			if err := c.wr.MyFlush(); err != nil {
				return
			}
			time2 := time.Since(t2)
			//resp.MyPrint("Server AppendInt time is", time0,
			//	"AppendBulk time is", time1,
			//	"Server Flush time is", time2,
			//	"Chunk body len is ", len(temp.Body))
			t0 := time.Now()
			time0 := t0.Sub(t)
			if err := logger(resp.LogServer2Client, temp.Cmd, temp.ReqId, temp.ChunkId, int64(time0), int64(time1), int64(time2), t0.UnixNano()); err != nil {
				fmt.Println("LogServer2Client err", err)
				return
			}

		}

		// flush buffer, return on errors
		if err := c.wr.Flush(); err != nil {
			return
		}
	}
}

// client peeking Cmd
func myPeekCmd(c *Client, fn func(string) error, channel chan string) {
	for {
		if err := c.peekCmd(fn, channel); err != nil {
			c.wr.AppendError("ERR " + err.Error())
			if !resp.IsProtocolError(err) {
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
	//fmt.Println("in the lambda server")
	srv.serveClient(newClient(cn))
}
